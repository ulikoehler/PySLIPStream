#!/usr/bin/env python3
"""
Example 10: Async Frame Monitor with Mock Connection

This example shows how to use AsyncFrameMonitor with a mock connection
for testing without requiring actual hardware or network connections.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slipspeed.async_slip import encode_packet_async
from slipspeed.async_streaming import AsyncFrameMonitor
from slipspeed.async_connections import AsyncConnection
from slipspeed.crc import append_crc32


class MockAsyncConnection(AsyncConnection):
    """Mock async connection for testing."""
    
    def __init__(self, frames_to_send=None):
        self.frames_to_send = frames_to_send or []
        self.index = 0
        self.sent_data = []
        self._open = True
    
    async def read(self, n: int = 4096) -> bytes:
        """Return next frame or empty bytes."""
        if not self.is_open():
            return b''
        
        if self.index < len(self.frames_to_send):
            frame = self.frames_to_send[self.index]
            self.index += 1
            return frame
        return b''
    
    async def write(self, data: bytes) -> int:
        """Store written data."""
        if not self.is_open():
            return 0
        self.sent_data.append(data)
        return len(data)
    
    async def close(self) -> None:
        """Close connection."""
        self._open = False
    
    def is_open(self) -> bool:
        """Check if open."""
        return self._open


async def frame_monitor_example():
    """Demonstrate AsyncFrameMonitor with mock connection."""
    
    print("=" * 60)
    print("Async Frame Monitor Example (Mock Connection)")
    print("=" * 60)
    
    # Prepare test frames
    frames = [
        b"Frame 1",
        b"Frame 2 with CRC",
        b"Frame 3",
    ]
    
    # Add CRC to some frames
    encoded_frames = []
    for i, frame in enumerate(frames):
        if i % 2 == 1:
            frame = append_crc32(frame)
        encoded = await encode_packet_async(frame)
        encoded_frames.append(encoded)
    
    print(f"\nPrepared {len(encoded_frames)} test frames")
    
    # Example 1: Basic monitoring with callback
    print("\n1. Basic monitoring with callback:")
    received_frames = []
    
    def on_frame(frame):
        received_frames.append(frame)
        print(f"  Received: {frame}")
    
    mock_conn = MockAsyncConnection(encoded_frames)
    monitor = AsyncFrameMonitor(mock_conn, frame_callback=on_frame, check_crc=True)
    
    await monitor.monitor(duration=0.5)  # Short duration since we have limited data
    print(f"  Frames received: {len(received_frames)}")
    
    # Example 2: Monitor with iteration (skip for mock connection)
    print("\n2. Monitor with async iteration (skipped for mock connection):")
    print("  Note: monitor_iter() is designed for continuous streams.")
    print("  For mock connections, use monitor() with duration instead.")
    
    # Example 3: Send frames
    print("\n3. Send frames:")
    mock_conn3 = MockAsyncConnection()
    monitor3 = AsyncFrameMonitor(mock_conn3, check_crc=False)
    
    test_payload = b"Test message"
    await monitor3.send_frame(test_payload)
    
    print(f"  Sent: {test_payload}")
    print(f"  Data in buffer: {len(mock_conn3.sent_data)} items")
    
    await monitor3.close()
    
    # Example 4: Statistics
    print("\n4. Statistics from monitor:")
    mock_conn4 = MockAsyncConnection(encoded_frames)
    monitor4 = AsyncFrameMonitor(mock_conn4, check_crc=True)
    
    await monitor4.monitor(duration=0.5)
    
    stats = monitor4.get_stats()
    print(f"  Total frames: {stats['frames_received']}")
    print(f"  Bad CRC: {stats['frames_with_bad_crc']}")
    print(f"  Errors: {stats['frames_with_errors']}")
    print(f"  Total bytes: {stats['total_bytes_received']}")
    
    monitor4.print_stats()
    await monitor4.close()
    
    print("\n" + "=" * 60)
    print("All frame monitor examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(frame_monitor_example())
