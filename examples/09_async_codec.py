#!/usr/bin/env python3
"""
Example 9: Async Codec (Rust-style)

This example shows how to use the AsyncSlipCodec, which follows the Rust
implementation's design pattern with stateful decoding from buffers.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slipspeed.async_slip import AsyncSlipCodec, encode_packet_async


async def codec_example():
    """Demonstrate AsyncSlipCodec usage."""
    
    print("=" * 60)
    print("AsyncSlipCodec Example (Rust-style)")
    print("=" * 60)
    
    codec = AsyncSlipCodec()
    
    # Example 1: Encode and decode single frame
    print("\n1. Single frame encode/decode:")
    data = b"Hello, Codec!"
    encoded = await codec.encode(data)
    print(f"  Original: {data}")
    print(f"  Encoded:  {encoded.hex()}")
    
    src = bytearray(encoded)
    decoded = await codec.decode(src)
    print(f"  Decoded:  {decoded}")
    print(f"  Buffer remaining: {len(src)} bytes")
    assert decoded == data
    print("  ✓ Round-trip successful")
    
    # Example 2: Decode multiple frames from buffer
    print("\n2. Multiple frames in buffer:")
    codec.reset()
    
    frame1 = b"Frame 1"
    frame2 = b"Frame 2"
    frame3 = b"Frame 3"
    
    # Encode all frames
    buffer = bytearray()
    buffer.extend(await codec.encode(frame1))
    buffer.extend(await codec.encode(frame2))
    buffer.extend(await codec.encode(frame3))
    
    print(f"  Buffer contains {len(buffer)} bytes")
    print(f"  Buffer hex: {buffer.hex()}")
    
    # Decode frames one by one
    frames_decoded = []
    while True:
        frame = await codec.decode(buffer)
        if frame is None:
            break
        frames_decoded.append(frame)
        print(f"  Decoded frame: {frame}")
    
    print(f"  Total frames decoded: {len(frames_decoded)}")
    print(f"  Buffer remaining: {len(buffer)} bytes")
    assert len(frames_decoded) == 3
    assert frames_decoded == [frame1, frame2, frame3]
    print("  ✓ All frames decoded correctly")
    
    # Example 3: Partial frame handling
    print("\n3. Partial frame handling:")
    codec.reset()
    
    partial_frame = await codec.encode(b"Partial data")
    # Split the frame
    part1 = partial_frame[:len(partial_frame)//2]
    part2 = partial_frame[len(partial_frame)//2:]
    
    src = bytearray(part1)
    decoded = await codec.decode(src)
    print(f"  After first part: decoded={decoded}, buffer={len(src)} bytes")
    assert decoded is None  # Should not have complete frame yet
    
    src.extend(part2)
    decoded = await codec.decode(src)
    print(f"  After second part: decoded={decoded}, buffer={len(src)} bytes")
    assert decoded == b"Partial data"
    print("  ✓ Partial frame handling works correctly")
    
    # Example 4: decode_eof for stream termination
    print("\n4. decode_eof for stream termination:")
    codec.reset()
    
    complete_frame = await codec.encode(b"Complete")
    src = bytearray(complete_frame)
    
    decoded = await codec.decode_eof(src)
    print(f"  Decoded with EOF: {decoded}")
    print(f"  Buffer remaining: {len(src)} bytes")
    assert decoded == b"Complete"
    print("  ✓ decode_eof works correctly")
    
    # Example 5: decode_eof with incomplete frame (error case)
    print("\n5. decode_eof with incomplete frame (should error):")
    codec.reset()
    
    incomplete = bytearray(await codec.encode(b"Test"))
    incomplete.pop()  # Remove END marker
    incomplete.pop()  # Remove last byte to make it incomplete
    
    try:
        await codec.decode_eof(incomplete)
        print("  ✗ Should have raised error")
    except ValueError as e:
        print(f"  ✓ Correctly raised error: {e}")
    
    # Example 6: Stateful decoding with special bytes
    print("\n6. Stateful decoding with special bytes:")
    codec.reset()
    
    special_data = bytes([0x01, 0xC0, 0xDB, 0x03])  # Contains END and ESC
    encoded_special = await codec.encode(special_data)
    
    src = bytearray(encoded_special)
    decoded = await codec.decode(src)
    
    print(f"  Original: {special_data.hex()}")
    print(f"  Encoded:  {encoded_special.hex()}")
    print(f"  Decoded:  {decoded.hex()}")
    assert decoded == special_data
    print("  ✓ Special bytes handled correctly")
    
    print("\n" + "=" * 60)
    print("All codec examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(codec_example())
