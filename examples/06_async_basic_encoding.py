"""
Async basic SLIP encoding and decoding example.

Demonstrates async SLIP packet encoding and decoding using the async API.
"""

import asyncio
from slipstream.async_slip import encode_packet_async, decode_packet_async, END, ESC


async def main():
    print("Async SLIP Encoding/Decoding Example")
    print("=" * 40)
    
    # Example 1: Simple encoding
    print("\n1. Simple encoding:")
    data = b"Hello, SLIP!"
    encoded = await encode_packet_async(data)
    print(f"   Original: {data}")
    print(f"   Encoded:  {encoded.hex()}")
    
    # Decode it back
    decoded, consumed = await decode_packet_async(encoded)
    print(f"   Decoded:  {decoded}")
    print(f"   Consumed: {consumed} bytes")
    
    # Example 2: Encoding with special bytes
    print("\n2. Encoding with special bytes (END, ESC):")
    special_data = bytes([0x01, 0x02, END, ESC, 0x03])
    encoded_special = await encode_packet_async(special_data)
    print(f"   Original: {special_data.hex()}")
    print(f"   Encoded:  {encoded_special.hex()}")
    
    # Decode it back
    decoded_special, consumed_special = await decode_packet_async(encoded_special)
    print(f"   Decoded:  {decoded_special.hex()}")
    print(f"   Consumed: {consumed_special} bytes")
    
    # Verify round-trip
    assert decoded_special == special_data, "Round-trip failed!"
    print("   ✓ Round-trip successful")
    
    # Example 3: Empty packet
    print("\n3. Empty packet:")
    empty_data = b""
    encoded_empty = await encode_packet_async(empty_data)
    print(f"   Original: {empty_data}")
    print(f"   Encoded:  {encoded_empty.hex()}")
    
    decoded_empty, consumed_empty = await decode_packet_async(encoded_empty)
    print(f"   Decoded:  {decoded_empty}")
    print(f"   Consumed: {consumed_empty} bytes")
    
    # Example 4: Large packet
    print("\n4. Large packet (1000 bytes):")
    large_data = bytes(range(256)) * 4  # 1024 bytes
    encoded_large = await encode_packet_async(large_data)
    print(f"   Original length: {len(large_data)} bytes")
    print(f"   Encoded length:  {len(encoded_large)} bytes")
    
    decoded_large, consumed_large = await decode_packet_async(encoded_large)
    print(f"   Decoded length:  {len(decoded_large)} bytes")
    
    assert decoded_large == large_data, "Large round-trip failed!"
    print("   ✓ Large round-trip successful")
    
    print("\n" + "=" * 40)
    print("All examples completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
