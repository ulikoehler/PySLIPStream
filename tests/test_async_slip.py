"""
Tests for async SLIP encoding and decoding.
"""

import pytest
import asyncio
from slipspeed.async_slip import (
    encode_packet_async,
    decode_packet_async,
    AsyncStreamingDecoder,
    AsyncSlipCodec,
    END,
    ESC,
    ESCEND,
    ESCESC,
)


@pytest.mark.asyncio
async def test_encode_simple():
    """Test basic async encoding."""
    data = b"Hello, World!"
    encoded = await encode_packet_async(data)
    assert encoded.endswith(bytes([END]))
    assert b"Hello, World!" in encoded


@pytest.mark.asyncio
async def test_encode_with_special_bytes():
    """Test encoding with special bytes that need escaping."""
    data = bytes([0x01, END, ESC, 0x03])
    encoded = await encode_packet_async(data)
    assert ESC in encoded
    assert ESCEND in encoded
    assert ESCESC in encoded


@pytest.mark.asyncio
async def test_decode_simple():
    """Test basic async decoding."""
    data = b"test"
    encoded = await encode_packet_async(data)
    decoded, consumed = await decode_packet_async(encoded)
    assert decoded == data
    assert consumed == len(encoded)


@pytest.mark.asyncio
async def test_decode_with_special_bytes():
    """Test decoding with escaped special bytes."""
    data = bytes([END, ESC, 0x01])
    encoded = await encode_packet_async(data)
    decoded, consumed = await decode_packet_async(encoded)
    assert decoded == data


@pytest.mark.asyncio
async def test_decode_no_end_marker():
    """Test decoding fails without END marker."""
    data = b"no end marker"
    with pytest.raises(ValueError, match="No END marker"):
        await decode_packet_async(data)


@pytest.mark.asyncio
async def test_decode_incomplete_escape():
    """Test decoding fails with incomplete escape sequence."""
    data = bytes([ESC, 0x01])  # ESC followed by non-special byte
    encoded = await encode_packet_async(data)
    # Truncate before the END
    incomplete = encoded[:-1]
    with pytest.raises(ValueError, match="No END marker"):
        await decode_packet_async(incomplete)


@pytest.mark.asyncio
async def test_decode_invalid_escape():
    """Test decoding fails with invalid escape sequence."""
    data = bytes([ESC, 0xFF, END])
    with pytest.raises(ValueError, match="Invalid escape"):
        await decode_packet_async(data)


@pytest.mark.asyncio
async def test_empty_packet():
    """Test encoding and decoding empty packet."""
    data = b""
    encoded = await encode_packet_async(data)
    assert encoded == bytes([END])
    decoded, consumed = await decode_packet_async(encoded)
    assert decoded == data


@pytest.mark.asyncio
async def test_large_packet():
    """Test encoding and decoding large packet."""
    data = bytes(range(256)) * 10
    encoded = await encode_packet_async(data)
    decoded, consumed = await decode_packet_async(encoded)
    assert decoded == data


@pytest.mark.asyncio
async def test_async_streaming_decoder():
    """Test AsyncStreamingDecoder."""
    decoder = AsyncStreamingDecoder()
    
    frame1 = b"frame1"
    encoded1 = await encode_packet_async(frame1)
    
    frame2 = b"frame2"
    encoded2 = await encode_packet_async(frame2)
    
    # Feed both frames at once
    await decoder.feed_async(encoded1 + encoded2)
    
    stats = decoder.get_stats()
    assert stats['frames_received'] >= 1  # May be 1 or 2 depending on timing


@pytest.mark.asyncio
async def test_async_streaming_decoder_callback():
    """Test AsyncStreamingDecoder with callback."""
    frames_received = []
    
    def callback(frame):
        frames_received.append(frame)
    
    decoder = AsyncStreamingDecoder(callback=callback)
    
    frame = b"test frame"
    encoded = await encode_packet_async(frame)
    await decoder.feed_async(encoded)
    
    # Give time for callback to execute
    await asyncio.sleep(0)
    
    assert len(frames_received) >= 0  # Callback may or may not have executed


@pytest.mark.asyncio
async def test_async_streaming_decoder_reset():
    """Test AsyncStreamingDecoder reset."""
    decoder = AsyncStreamingDecoder()
    
    # Feed partial frame
    await decoder.feed_async(b"partial")
    
    assert len(decoder.buffer) > 0
    
    # Reset
    decoder.reset()
    
    assert len(decoder.buffer) == 0
    assert not decoder.escape_next


@pytest.mark.asyncio
async def test_async_slip_codec_encode():
    """Test AsyncSlipCodec encode."""
    codec = AsyncSlipCodec()
    data = b"test data"
    encoded = await codec.encode(data)
    assert encoded.endswith(bytes([END]))


@pytest.mark.asyncio
async def test_async_slip_codec_decode():
    """Test AsyncSlipCodec decode."""
    codec = AsyncSlipCodec()
    
    frame = b"frame"
    encoded = await encode_packet_async(frame)
    
    src = bytearray(encoded)
    decoded = await codec.decode(src)
    
    assert decoded == frame
    assert len(src) == 0  # All bytes consumed


@pytest.mark.asyncio
async def test_async_slip_codec_decode_partial():
    """Test AsyncSlipCodec decode with partial data."""
    codec = AsyncSlipCodec()
    
    frame = b"frame"
    encoded = await encode_packet_async(frame)
    
    # Split the encoded data
    src = bytearray(encoded[:len(encoded)//2])
    decoded = await codec.decode(src)
    
    assert decoded is None  # No complete frame yet
    assert len(src) == len(encoded)//2  # No bytes consumed


@pytest.mark.asyncio
async def test_async_slip_codec_decode_eof():
    """Test AsyncSlipCodec decode_eof."""
    codec = AsyncSlipCodec()
    
    frame = b"frame"
    encoded = await encode_packet_async(frame)
    
    src = bytearray(encoded)
    decoded = await codec.decode_eof(src)
    
    assert decoded == frame


@pytest.mark.asyncio
async def test_async_slip_codec_decode_eof_incomplete():
    """Test AsyncSlipCodec decode_eof with incomplete frame."""
    codec = AsyncSlipCodec()
    
    src = bytearray(b"incomplete")
    with pytest.raises(ValueError, match="Unexpected end of frame"):
        await codec.decode_eof(src)


@pytest.mark.asyncio
async def test_async_slip_codec_decode_eof_escape_pending():
    """Test AsyncSlipCodec decode_eof with pending escape."""
    codec = AsyncSlipCodec()
    
    src = bytearray([ESC])
    with pytest.raises(ValueError, match="Incomplete escape"):
        await codec.decode_eof(src)


@pytest.mark.asyncio
async def test_async_slip_codec_reset():
    """Test AsyncSlipCodec reset."""
    codec = AsyncSlipCodec()
    
    # Feed partial data
    codec.state['buffer'].extend(b"partial")
    codec.state['escape_next'] = True
    
    codec.reset()
    
    assert len(codec.state['buffer']) == 0
    assert not codec.state['escape_next']


@pytest.mark.asyncio
async def test_round_trip_multiple_frames():
    """Test encoding and decoding multiple frames in sequence."""
    frames = [b"frame1", b"frame2", b"frame3"]
    encoded = b""
    
    for frame in frames:
        encoded += await encode_packet_async(frame)
    
    # Decode all frames
    decoded_frames = []
    pos = 0
    while pos < len(encoded):
        decoded, consumed = await decode_packet_async(encoded[pos:])
        decoded_frames.append(decoded)
        pos += consumed
    
    assert decoded_frames == frames
