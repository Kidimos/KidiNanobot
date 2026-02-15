"""Discord channel implementation using Discord Gateway websocket."""

import asyncio
import json
import re
from pathlib import Path
from typing import Any

import httpx
import websockets
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import DiscordConfig

DISCORD_API_BASE = "https://discord.com/api/v10"
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024  # 20MB


class DiscordChannel(BaseChannel):
    """Discord channel using Gateway websocket."""

    name = "discord"

    def __init__(self, config: DiscordConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: DiscordConfig = config
        self._ws: websockets.ClientConnection | None = None
        self._seq: int | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._http: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """Start the Discord gateway connection."""
        if not self.config.token:
            logger.error("Discord bot token not configured")
            return

        self._running = True
        self._http = httpx.AsyncClient(timeout=30.0)

        while self._running:
            try:
                logger.info("Connecting to Discord gateway...")
                async with websockets.connect(self.config.gateway_url) as ws:
                    self._ws = ws
                    await self._gateway_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Discord gateway error: {e}")
                if self._running:
                    logger.info("Reconnecting to Discord gateway in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the Discord channel."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        for task in self._typing_tasks.values():
            task.cancel()
        self._typing_tasks.clear()
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http:
            await self._http.aclose()
            self._http = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Discord REST API."""
        if not self._http:
            logger.warning("Discord HTTP client not initialized")
            return

        # 1.发送前验证消息内容
        if not msg.content:
            logger.error("Attempted to send empty message to Discord")
            return
        if len(msg.content) > 2000:
            logger.debug("Message exceeds Discord 2000 character limit: {len(msg.content)}")

        chunks = self._split_message(msg.content)
        logger.debug("Splitting messages into {len(chunks)} chunks")

        for i, chunk in enumerate(chunks):
            payload: dict[str, Any] = {"content": chunk}
            if i == 0 and msg.reply_to:
                payload["message_reference"] = {"message_id": msg.reply_to}
                payload["allowed_mentions"] = {"replied_user": False}

            headers = {"Authorization": f"Bot {self.config.token}"}
            url = f"{DISCORD_API_BASE}/channels/{msg.chat_id}/messages"

            logger.debug(f"Sending chunk {i + 1}/{len(chunks)} to Discord: {json.dumps(payload)}")

            try:
                for attempt in range(3):
                    try:
                        response = await self._http.post(url, headers=headers, json=payload)
                        if response.status_code == 429:
                            data = response.json()
                            retry_after = float(data.get("retry_after", 1.0))
                            logger.warning(f"Discord rate limited, retrying in {retry_after}s")
                            await asyncio.sleep(retry_after)
                            continue
                        response.raise_for_status()
                        break
                    except Exception as e:
                        if attempt == 2:
                            logger.error(f"Error sending Discord message chunk{i + 1}: {e}")
                        else:
                            await asyncio.sleep(1)
            finally:
                if i == len(chunks) - 1:
                    await self._stop_typing(msg.chat_id)

    async def _gateway_loop(self) -> None:
        """Main gateway loop: identify, heartbeat, dispatch events."""
        if not self._ws:
            return

        async for raw in self._ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON from Discord gateway: {raw[:100]}")
                continue

            op = data.get("op")
            event_type = data.get("t")
            seq = data.get("s")
            payload = data.get("d")

            if seq is not None:
                self._seq = seq

            if op == 10:
                # HELLO: start heartbeat and identify
                interval_ms = payload.get("heartbeat_interval", 45000)
                await self._start_heartbeat(interval_ms / 1000)
                await self._identify()
            elif op == 0 and event_type == "READY":
                logger.info("Discord gateway READY")
            elif op == 0 and event_type == "MESSAGE_CREATE":
                await self._handle_message_create(payload)
            elif op == 7:
                # RECONNECT: exit loop to reconnect
                logger.info("Discord gateway requested reconnect")
                break
            elif op == 9:
                # INVALID_SESSION: reconnect
                logger.warning("Discord gateway invalid session")
                break

    async def _identify(self) -> None:
        """Send IDENTIFY payload."""
        if not self._ws:
            return

        identify = {
            "op": 2,
            "d": {
                "token": self.config.token,
                "intents": self.config.intents,
                "properties": {
                    "os": "nanobot",
                    "browser": "nanobot",
                    "device": "nanobot",
                },
            },
        }
        await self._ws.send(json.dumps(identify))

    async def _start_heartbeat(self, interval_s: float) -> None:
        """Start or restart the heartbeat loop."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        async def heartbeat_loop() -> None:
            while self._running and self._ws:
                payload = {"op": 1, "d": self._seq}
                try:
                    await self._ws.send(json.dumps(payload))
                except Exception as e:
                    logger.warning(f"Discord heartbeat failed: {e}")
                    break
                await asyncio.sleep(interval_s)

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())

    async def _handle_message_create(self, payload: dict[str, Any]) -> None:
        """Handle incoming Discord messages."""
        author = payload.get("author") or {}
        if author.get("bot"):
            return

        sender_id = str(author.get("id", ""))
        channel_id = str(payload.get("channel_id", ""))
        content = payload.get("content") or ""

        if not sender_id or not channel_id:
            return

        if not self.is_allowed(sender_id):
            return

        content_parts = [content] if content else []
        media_paths: list[str] = []
        media_dir = Path.home() / ".nanobot" / "media"

        for attachment in payload.get("attachments") or []:
            url = attachment.get("url")
            filename = attachment.get("filename") or "attachment"
            size = attachment.get("size") or 0
            if not url or not self._http:
                continue
            if size and size > MAX_ATTACHMENT_BYTES:
                content_parts.append(f"[attachment: {filename} - too large]")
                continue
            try:
                media_dir.mkdir(parents=True, exist_ok=True)
                file_path = (
                    media_dir / f"{attachment.get('id', 'file')}_{filename.replace('/', '_')}"
                )
                resp = await self._http.get(url)
                resp.raise_for_status()
                file_path.write_bytes(resp.content)
                media_paths.append(str(file_path))
                content_parts.append(f"[attachment: {file_path}]")
            except Exception as e:
                logger.warning(f"Failed to download Discord attachment: {e}")
                content_parts.append(f"[attachment: {filename} - download failed]")

        reply_to = (payload.get("referenced_message") or {}).get("id")

        await self._start_typing(channel_id)

        await self._handle_message(
            sender_id=sender_id,
            chat_id=channel_id,
            content="\n".join(p for p in content_parts if p) or "[empty message]",
            media=media_paths,
            metadata={
                "message_id": str(payload.get("id", "")),
                "guild_id": payload.get("guild_id"),
                "reply_to": reply_to,
            },
        )

    async def _start_typing(self, channel_id: str) -> None:
        """Start periodic typing indicator for a channel."""
        await self._stop_typing(channel_id)

        async def typing_loop() -> None:
            url = f"{DISCORD_API_BASE}/channels/{channel_id}/typing"
            headers = {"Authorization": f"Bot {self.config.token}"}
            while self._running:
                try:
                    if not self._http:
                        logger.warning("HTTP client unaviilable, stopping typing loop")
                        break

                    await self._http.post(url, headers=headers)
                except Exception:
                    pass
                await asyncio.sleep(8)

        self._typing_tasks[channel_id] = asyncio.create_task(typing_loop())

    async def _stop_typing(self, channel_id: str) -> None:
        """Stop typing indicator for a channel."""
        task = self._typing_tasks.pop(channel_id, None)
        if task:
            task.cancel()

    def _split_message(self, text: str, max_length: int = 2000) -> list[str]:
        """Split a long message into chunks, preserving code blocks as much as possible."""
        if len(text) <= max_length:
            return [text]

        # Step 1: Find all code block ranges
        code_block_ranges = []
        pattern = re.compile(r"```(?:\w*)\n(.*?)```", re.DOTALL)
        pos = 0
        while True:
            match = pattern.search(text, pos)
            if not match:
                break
            start, end = match.start(), match.end()
            code_block_ranges.append((start, end))
            pos = end

        # Step 2: Split text into segments (alternating non-code and code)
        segments = []
        last_end = 0
        for start, end in code_block_ranges:
            if last_end < start:
                segments.append(("text", text[last_end:start]))
            segments.append(("code", text[start:end]))
            last_end = end
        if last_end < len(text):
            segments.append(("text", text[last_end:]))

        # Step 3: Expand each segment into subsegments(each <= max_length)
        expanded = []
        for seg_type, seg_text in segments:
            if len(seg_text) <= max_length:
                expanded.append((seg_type, seg_text))
            else:
                if seg_type == "text":
                    # split_normal_text already returns chunks ≤ max_length
                    sub_texts = self._split_normal_text(seg_text, max_length)
                    for sub in sub_texts:
                        expanded.append(("text", sub))
                else:  # code
                    sub_codes = self._split_code_block(seg_text, max_length)
                    for sub in sub_codes:
                        expanded.append(("code", sub))

        # Step 3: Greedily merge consecutive subsegments
        result_chunks = []
        current_parts = []
        current_len = 0
        for seg_type, seg_text in expanded:
            seg_len = len(seg_text)
            if current_len + seg_len <= max_length:
                current_parts.append(seg_text)
                current_len += seg_len
            else:
                if current_parts:
                    result_chunks.append("".join(current_parts))
                current_parts = [seg_text]
                current_len = seg_len
        if current_parts:
            result_chunks.append("".join(current_parts))

        return result_chunks

    def _split_normal_text(self, text: str, max_length: int) -> list[str]:
        """Split plain text with priority: paragraph, line, space, hard."""
        if len(text) <= max_length:
            return [text]

        chunks = []
        start = 0
        text_len = len(text)

        while start < text_len:
            end = min(start + max_length, text_len)

            if end == text_len:
                chunks.append(text[start:])
                break

            # 1. Try paragraph break (\n\n)
            split_at = text.rfind("\n\n", start, end)
            # Avoid splitting too early (use at least half of the max length)
            if split_at > start + max_length // 2:
                chunks.append(text[start:split_at])
                start = split_at + 2  # skip the two newlines
                continue

            # 2. Try line break (\n)
            split_at = text.rfind("\n", start, end)
            if split_at > start:
                chunks.append(text[start:split_at])
                start = split_at + 1
                continue

            # 3. Try space
            split_at = text.rfind(" ", start, end)
            if split_at > start:
                chunks.append(text[start:split_at])
                start = split_at + 1
                continue

            # 4. Hard split (no suitable boundary found)
            chunks.append(text[start:end])
            start = end

        return chunks

    def _split_code_block(self, code: str, max_length: int) -> list[str]:
        """Split a code block into multiple valid code blocks."""
        lines = code.splitlines(keepends=True)
        if not lines:
            return []

        first_line = lines[0]  # e.g. "```python\n" or "```\n"
        last_line = lines[-1]  # "```"
        content_lines = lines[1:-1]  # may be empty

        # If the code block has no content, return as is
        if not content_lines:
            return [code]

        result = []
        current_block_lines = []
        # The fixed overhead: first line + last line
        overhead = len(first_line) + len(last_line)
        current_size = overhead

        for line in content_lines:
            line_len = len(line)
            if current_size + line_len > max_length:
                # Current block would exceed limit
                if current_block_lines:
                    # Finish current block
                    block = first_line + "".join(current_block_lines) + last_line
                    result.append(block)
                    # Start new block with this line
                    current_block_lines = [line]
                    current_size = overhead + line_len
                else:
                    # A single line is already too long – hard split the line itself
                    # We'll split the line into chunks and wrap each with code fences.
                    # This is an extreme case; we'll handle it by splitting the line
                    # and making each chunk a separate code block.
                    remaining = line
                    while remaining:
                        # Calculate available space for content
                        available = max_length - overhead
                        if available <= 0:
                            # Overhead alone exceeds limit – impossible, but fallback
                            # Just return the whole thing (will probably fail)
                            result.append(code)
                            break

                        # Take a chunk of the line
                        chunk = remaining[:available]
                        result.append(first_line + chunk + last_line)
                        remaining = remaining[available:]
                    # Reset for next line
                    current_block_lines = []
                    current_size = overhead
            else:
                current_block_lines.append(line)
                current_size += line_len

        # Add the last block
        if current_block_lines:
            block = first_line + "".join(current_block_lines) + last_line
            result.append(block)

        return result
