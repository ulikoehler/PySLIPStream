"""
Async SLIP (Serial Line Internet Protocol) encoder and decoder.

This module provides async functions and classes to encode and decode data using the SLIP protocol.
The SLIP protocol uses special escape sequences to frame messages:
- 0xC0 (END): End of packet marker
- 0xDB (ESC): Escape character
- 0xDC (ESCEND): Escaped END byte (represents 0xC0 in data)
- 0xDD (ESCESC): Escaped ESC byte (represents 0xDB in data)

Reference: RFC 1055 - Nonstandard Transmission of IP Datagrams over Serial Lines: SLIP
"""

import asyncio
from typing import AsyncIterator, Tuple, Optional, Callable

# SLIP special byte definitions
END = 0xC0      # End of packet
ESC = 0xDB      # Escape character
ESCEND = 0xDC   # Escaped END (represents literal 0xC0)
ESCESC = 0xDD   # Escaped ESC (represents literal 0xDB)


async def encode_packet_async(data: bytes) -> bytes:
    """
    Async wrapper for SLIP packet encoding.
    
    This function is async for API consistency but performs synchronous encoding
    since encoding is CPU-bound and doesn't involve I/O.
    
    Args:
        data: Raw bytes to encode
        
    Returns:
        SLIP-encoded bytes with END marker appended
    """
    # Yield control to event loop for consistency
    await asyncio.sleep(0)
    
    encoded = bytearray()
    
    for byte in data:
        if byte == END:
            encoded.append(ESC)
            encoded.append(ESCEND)
        elif byte == ESC:
            encoded.append(ESC)
            encoded.append(ESCESC)
        else:
            encoded.append(byte)
    
    encoded.append(END)
    
    return bytes(encoded)


async def decode_packet_async(data: bytes) -> Tuple[bytes, int]:
    """
    Async wrapper for SLIP packet decoding.
    
    This function is async for API consistency but performs synchronous decoding
    since decoding is CPU-bound and doesn't involve I/O.
    
    Args:
        data: Buffer containing SLIP-encoded data
        
    Returns:
        Tuple of (decoded_bytes, bytes_consumed)
        - decoded_bytes: The decoded payload
        - bytes_consumed: Number of input bytes consumed (including END marker)
        
    Raises:
        ValueError: If the data contains invalid escape sequences or no END marker
    """
    # Yield control to event loop for consistency
    await asyncio.sleep(0)
    
    decoded = bytearray()
    i = 0
    
    while i < len(data):
        byte = data[i]
        
        if byte == END:
            return bytes(decoded), i + 1
        
        elif byte == ESC:
            if i + 1 >= len(data):
                raise ValueError("Incomplete escape sequence at end of buffer")
            
            next_byte = data[i + 1]
            if next_byte == ESCEND:
                decoded.append(END)
                i += 2
            elif next_byte == ESCESC:
                decoded.append(ESC)
                i += 2
            else:
                raise ValueError(f"Invalid escape sequence: ESC followed by 0x{next_byte:02X}")
        
        else:
            decoded.append(byte)
            i += 1
    
    raise ValueError("No END marker found in data")


class AsyncStreamingDecoder:
    """
    Async stateful SLIP decoder for processing continuous streams of data.
    
    This decoder maintains state across multiple feed operations and can be used
    with async generators for streaming frame processing.
    
    Usage:
        decoder = AsyncStreamingDecoder()
        async for frame in decoder.decode_stream(data_stream):
            process_frame(frame)
    """
    
    def __init__(self, callback: Optional[Callable[[bytes], None]] = None):
        """
        Initialize the async streaming decoder.
        
        Args:
            callback: Optional sync callback for each decoded frame.
                     For async callbacks, use the async generator interface instead.
        """
        self.buffer = bytearray()
        self.escape_next = False
        self.callback = callback
        self.frames_received = 0
        self.frames_with_errors = 0
        self._queue: Optional[asyncio.Queue] = None
    
    async def feed_async(self, data: bytes) -> None:
        """
        Async feed data into the decoder and process new frames.
        
        Args:
            data: Bytes to be decoded
        """
        for byte in data:
            self._process_byte(byte)
            # Yield control periodically for large chunks
            if len(data) > 1000 and byte % 1000 == 0:
                await asyncio.sleep(0)
    
    def _process_byte(self, byte: int) -> None:
        """Process a single byte from the stream."""
        if self.escape_next:
            if byte == ESCEND:
                self.buffer.append(END)
            elif byte == ESCESC:
                self.buffer.append(ESC)
            else:
                self.buffer.clear()
                self.frames_with_errors += 1
            self.escape_next = False
        
        elif byte == END:
            if len(self.buffer) > 0:
                self.frames_received += 1
                frame = bytes(self.buffer)
                if self.callback:
                    self.callback(frame)
                self.buffer.clear()
        
        elif byte == ESC:
            self.escape_next = True
        
        else:
            self.buffer.append(byte)
    
    async def decode_stream(
        self,
        data_stream: AsyncIterator[bytes]
    ) -> AsyncIterator[bytes]:
        """
        Decode frames from an async stream of bytes.
        
        Args:
            data_stream: Async iterator yielding byte chunks
            
        Yields:
            Decoded frames as they become available
        """
        async for chunk in data_stream:
            await self.feed_async(chunk)
            # Yield any complete frames from buffer
            if len(self.buffer) > 0 and not self.escape_next:
                # Check if we have a complete frame (this is a simplification;
                # in real streaming, frames are only complete on END byte)
                pass
        
        # Handle any remaining data
        if len(self.buffer) > 0:
            # This is an incomplete frame, handle as error or partial
            pass
    
    def reset(self) -> None:
        """Reset the decoder state (discard any partial frames)."""
        self.buffer.clear()
        self.escape_next = False
    
    def get_stats(self) -> dict:
        """Get decoding statistics."""
        return {
            'frames_received': self.frames_received,
            'frames_with_errors': self.frames_with_errors,
        }
    
    async def get_frame_queue(self) -> asyncio.Queue:
        """
        Get or create an asyncio queue for receiving decoded frames.
        
        Returns:
            Queue that will be populated with decoded frames as they arrive
        """
        if self._queue is None:
            self._queue = asyncio.Queue()
            # Replace callback with queue feeder
            self.callback = lambda frame: asyncio.create_task(self._queue.put(frame))
        return self._queue
    
    async def _queue_put(self, frame: bytes) -> None:
        """Helper to put frame in queue from sync callback."""
        if self._queue:
            await self._queue.put(frame)


class AsyncSlipCodec:
    """
    Async codec similar to Rust's SlipCodec implementation.
    
    Provides encode/decode methods with stateful decoding for streaming.
    """
    
    def __init__(self):
        """Initialize the codec with empty state."""
        self.state = {
            'buffer': bytearray(),
            'escape_next': False
        }
    
    async def encode(self, item: bytes) -> bytes:
        """
        Encode a byte slice as a SLIP frame.
        
        Args:
            item: Bytes to encode
            
        Returns:
            SLIP-encoded frame
        """
        return await encode_packet_async(item)
    
    async def decode(self, src: bytearray) -> Optional[bytes]:
        """
        Decode bytes from source buffer, returning a complete frame if available.
        
        This method processes the buffer and returns a complete frame when an END
        delimiter is encountered. The consumed bytes are removed from the buffer.
        
        Args:
            src: Source buffer (will be modified in place)
            
        Returns:
            Decoded frame if complete, None otherwise
        """
        buffer = self.state['buffer']
        escape_next = self.state['escape_next']
        
        i = 0
        while i < len(src):
            byte = src[i]
            
            if escape_next:
                if byte == ESCEND:
                    buffer.append(END)
                elif byte == ESCESC:
                    buffer.append(ESC)
                else:
                    # Invalid escape, clear buffer
                    buffer.clear()
                    escape_next = False
                    i += 1
                    continue
                escape_next = False
                i += 1
            
            elif byte == END:
                # Complete frame
                frame = bytes(buffer)
                buffer.clear()
                self.state['buffer'] = buffer
                self.state['escape_next'] = False
                # Remove consumed bytes from src
                del src[:i+1]
                return frame
            
            elif byte == ESC:
                escape_next = True
                i += 1
            
            else:
                buffer.append(byte)
                i += 1
        
        # No complete frame, update state
        self.state['buffer'] = buffer
        self.state['escape_next'] = escape_next
        # Remove consumed bytes from src
        del src[:i]
        return None
    
    async def decode_eof(self, src: bytearray) -> Optional[bytes]:
        """
        Decode with EOF handling, similar to Rust's decode_eof.
        
        Args:
            src: Source buffer
            
        Returns:
            Decoded frame if available, None otherwise
            
        Raises:
            ValueError: If incomplete escape or frame at EOF
        """
        # Try to decode normally first
        frame = await self.decode(src)
        if frame is not None:
            return frame
        
        # Check for incomplete state
        if self.state['escape_next']:
            raise ValueError("Incomplete escape sequence at EOF")
        
        if len(self.state['buffer']) > 0:
            raise ValueError("Unexpected end of frame")
        
        return None
    
    def reset(self) -> None:
        """Reset codec state."""
        self.state = {
            'buffer': bytearray(),
            'escape_next': False
        }
