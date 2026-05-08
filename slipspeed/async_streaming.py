"""
Async high-level SLIP frame monitoring interface.

Combines async connection handling, SLIP decoding, CRC validation, and statistics.
"""

import asyncio
import struct
import time
from typing import Optional, Callable, Dict, Any, AsyncIterator
from .async_slip import AsyncStreamingDecoder, encode_packet_async
from .crc import extract_crc32, verify_crc32, calculate_crc32, diagnose_crc_error
from .stats import FrameStatistics
from .async_connections import AsyncConnection


class AsyncFrameMonitor:
    """
    Async high-level SLIP frame monitor with CRC validation and statistics.
    """
    
    def __init__(
        self,
        connection: AsyncConnection,
        frame_callback: Optional[Callable[[bytes], None]] = None,
        check_crc: bool = True,
        hex_output: bool = False
    ):
        """
        Initialize async frame monitor.
        
        Args:
            connection: AsyncConnection object
            frame_callback: Optional callback for each complete frame
                           Signature: callback(frame_data: bytes)
            check_crc: Whether to validate CRC32 on frames (expects last 4 bytes to be CRC)
            hex_output: Whether to display frames in hex format
        """
        self.connection = connection
        self.frame_callback = frame_callback
        self.check_crc = check_crc
        self.hex_output = hex_output
        
        self.stats = FrameStatistics()
        self.last_frame = None
        self.last_frame_crc_valid = None
        self._running = False
        self._queue: Optional[asyncio.Queue] = None
        
        # Create async streaming decoder
        self.decoder = AsyncStreamingDecoder(callback=self._handle_frame)
    
    def _handle_frame(self, frame_data: bytes) -> None:
        """Internal callback for decoded frames."""
        crc_valid = None
        crc_received = None
        crc_expected = None
        crc_diagnostic = None
        payload = frame_data
        
        # Check CRC if enabled
        if self.check_crc and len(frame_data) >= 4:
            try:
                payload, crc_bytes = extract_crc32(frame_data)
                crc_valid = verify_crc32(payload, crc_bytes)
                crc_received = int.from_bytes(crc_bytes, 'little')
                crc_expected = calculate_crc32(payload)
                if not crc_valid:
                    diag = diagnose_crc_error(payload, crc_bytes)
                    crc_diagnostic = diag.get('diagnosis')
            except (ValueError, struct.error):
                crc_valid = False
                crc_diagnostic = 'Frame too short or invalid CRC field'
        
        # Record statistics
        self.stats.add_frame(
            raw_frame_len=len(frame_data),
            payload_len=len(payload),
            crc_valid=crc_valid
        )
        
        self.last_frame = {
            'raw': frame_data,
            'payload': payload,
            'crc_valid': crc_valid,
            'crc_received': crc_received,
            'crc_expected': crc_expected,
            'crc_diagnostic': crc_diagnostic,
            'timestamp': time.time()
        }
        
        # Call user callback if provided
        if self.frame_callback:
            self.frame_callback(frame_data)
    
    async def process_chunk(self, chunk: bytes) -> None:
        """
        Process a chunk of raw data from the connection.
        
        Args:
            chunk: Raw bytes from the connection
        """
        if chunk:
            await self.decoder.feed_async(chunk)
    
    async def monitor(self, duration: Optional[float] = None) -> None:
        """
        Monitor the connection for SLIP frames.
        
        Args:
            duration: Monitor for this many seconds (None for infinite)
        """
        self._running = True
        start_time = time.time()
        
        try:
            while self._running:
                # Check timeout
                if duration is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= duration:
                        break
                
                # Read data from connection
                try:
                    chunk = await self.connection.read(n=4096)
                    if chunk:
                        await self.process_chunk(chunk)
                    else:
                        # Connection closed or no data
                        await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"Error reading from connection: {e}")
                    break
        finally:
            self._running = False
    
    async def monitor_iter(self) -> AsyncIterator[Dict[str, Any]]:
        """
        Monitor the connection and yield frames as they arrive.
        
        Yields:
            Dictionary containing frame information with keys:
            - raw: Raw frame bytes
            - payload: Decoded payload (with CRC removed if check_crc=True)
            - crc_valid: Boolean indicating CRC validity (None if not checked)
            - crc_received: Received CRC value (None if not checked)
            - crc_expected: Expected CRC value (None if not checked)
            - timestamp: Unix timestamp when frame was received
        """
        self._running = True
        
        # Create a queue for frame delivery
        if self._queue is None:
            self._queue = asyncio.Queue()
        
        # Replace callback with queue feeder
        original_callback = self.frame_callback
        self.frame_callback = lambda frame: asyncio.create_task(self._queue.put(frame))
        
        # Start monitoring in background
        monitor_task = asyncio.create_task(self.monitor(duration=None))
        
        try:
            while self._running:
                try:
                    frame_data = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                    # Get the last frame info
                    if self.last_frame and self.last_frame['raw'] == frame_data:
                        yield self.last_frame
                except asyncio.TimeoutError:
                    # No frame received, continue
                    continue
                except asyncio.CancelledError:
                    break
        finally:
            self._running = False
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            self.frame_callback = original_callback
    
    def stop(self) -> None:
        """Stop the monitor."""
        self._running = False
    
    async def send_frame(self, payload: bytes, append_crc: bool = False) -> int:
        """
        Encode and send a SLIP frame.
        
        Args:
            payload: Data to send
            append_crc: Whether to append CRC32 before encoding
            
        Returns:
            Number of bytes written
        """
        if append_crc:
            from .crc import append_crc32
            payload = append_crc32(payload)
        
        encoded = await encode_packet_async(payload)
        return await self.connection.write(encoded)
    
    def get_last_frame(self) -> Optional[Dict[str, Any]]:
        """Get the last received frame."""
        return self.last_frame
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        return self.stats.get_stats()
    
    def print_stats(self) -> None:
        """Print statistics report."""
        self.stats.print_report()
    
    async def close(self) -> None:
        """Close the connection."""
        self.stop()
        await self.connection.close()


async def create_async_monitor(
    connection_string: str,
    check_crc: bool = True,
    frame_callback: Optional[Callable[[bytes], None]] = None
) -> AsyncFrameMonitor:
    """
    Convenience function to create an async frame monitor from a connection string.
    
    Args:
        connection_string: Connection string for create_async_connection
        check_crc: Whether to validate CRC32 on frames
        frame_callback: Optional callback for each complete frame
        
    Returns:
        AsyncFrameMonitor instance
    """
    from .async_connections import create_async_connection
    
    connection = await create_async_connection(connection_string)
    return AsyncFrameMonitor(
        connection=connection,
        check_crc=check_crc,
        frame_callback=frame_callback
    )
