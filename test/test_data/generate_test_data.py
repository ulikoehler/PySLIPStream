#!/usr/bin/env python3
"""
Generate test data files with valid CRC32 checksums.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from slipstream import encode_packet, append_crc32

def create_frame_file(filepath: str, payload: bytes):
    """Create a SLIP frame file with valid CRC32."""
    # Append CRC32 to payload
    payload_with_crc = append_crc32(payload)
    # Encode as SLIP packet
    encoded = encode_packet(payload_with_crc)
    # Write to file
    with open(filepath, 'wb') as f:
        f.write(encoded)
    print(f"Created {filepath}: {len(payload)} bytes payload, {len(encoded)} bytes encoded")

if __name__ == '__main__':
    test_data_dir = Path(__file__).parent
    
    # Fix simple_frame.bin with "hello" and valid CRC
    create_frame_file(str(test_data_dir / 'simple_frame.bin'), b'hello')
    
    # Create new sample with ASCII text
    text = b'The brown dog jumps over the lazy fox'
    create_frame_file(str(test_data_dir / 'ascii_text.bin'), text)
    
    # Create a longer ASCII sample
    long_text = b'The quick brown fox jumps over the lazy dog. ' \
                b'The brown dog chases the cat up the tree. ' \
                b'Blabla blabla blabla - more text here!'
    create_frame_file(str(test_data_dir / 'ascii_long.bin'), long_text)
    
    print("\nAll test data files generated successfully!")
