#!/usr/bin/env python3
"""
Example 8: Async Streaming Decoder

This example shows how to use the AsyncStreamingDecoder for processing
continuous streams of SLIP-encoded data.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slipstream.async_slip import AsyncStreamingDecoder, encode_packet_async


async def streaming_decoder_example():
    """Demonstrate async streaming decoder."""
    
    print("=" * 60)
    print("Async Streaming Decoder Example")
    print("=" * 60)
    
    # Create streaming decoder with callback
    frames_received = []
    
    def on_frame(frame):
        frames_received.append(frame)
        print(f"  Received frame: {frame}")
    
    decoder = AsyncStreamingDecoder(callback=on_frame)
    
    # Example 1: Feed complete frames
    print("\n1. Feeding complete frames:")
    frame1 = b"First frame"
    frame2 = b"Second frame"
    frame3 = b"Third frame"
    
    encoded1 = await encode_packet_async(frame1)
    encoded2 = await encode_packet_async(frame2)
    encoded3 = await encode_packet_async(frame3)
    
    # Feed all at once
    await decoder.feed_async(encoded1 + encoded2 + encoded3)
    await asyncio.sleep(0.1)  # Give time for callbacks
    
    print(f"  Frames received: {len(frames_received)}")
    
    # Example 2: Feed data in chunks (simulating streaming)
    print("\n2. Feeding data in chunks (simulating stream):")
    decoder.reset()
    frames_received.clear()
    
    large_frame = b"This is a larger frame that might come in chunks"
    encoded_large = await encode_packet_async(large_frame)
    
    # Split into chunks
    chunk_size = 5
    for i in range(0, len(encoded_large), chunk_size):
        chunk = encoded_large[i:i+chunk_size]
        await decoder.feed_async(chunk)
        print(f"  Fed chunk {i//chunk_size + 1}: {chunk.hex()}")
        await asyncio.sleep(0.01)  # Simulate network delay
    
    await asyncio.sleep(0.1)
    print(f"  Frames received: {len(frames_received)}")
    
    # Example 3: Multiple frames interleaved
    print("\n3. Multiple frames interleaved:")
    decoder.reset()
    frames_received.clear()
    
    frames = [b"Alpha", b"Beta", b"Gamma", b"Delta"]
    all_encoded = b""
    
    for frame in frames:
        all_encoded += await encode_packet_async(frame)
    
    # Feed in random-sized chunks
    import random
    random.seed(42)  # For reproducibility
    pos = 0
    chunk_num = 1
    
    while pos < len(all_encoded):
        chunk_size = random.randint(3, 10)
        chunk = all_encoded[pos:pos+chunk_size]
        await decoder.feed_async(chunk)
        print(f"  Chunk {chunk_num}: {chunk.hex()}")
        pos += chunk_size
        chunk_num += 1
    
    await asyncio.sleep(0.1)
    print(f"  Frames received: {len(frames_received)}")
    print(f"  Expected: {len(frames)}")
    
    # Example 4: Statistics
    print("\n4. Decoder statistics:")
    stats = decoder.get_stats()
    print(f"  Frames received: {stats['frames_received']}")
    print(f"  Frames with errors: {stats['frames_with_errors']}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(streaming_decoder_example())
