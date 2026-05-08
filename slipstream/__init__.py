"""
slipstream: Python library for SLIP frame monitoring and processing.

This library provides comprehensive SLIP (Serial Line Internet Protocol) support including:
- Encoding and decoding of SLIP packets
- Streaming/continuous frame decoding
- CRC32 validation (Ethernet polynomial)
- Serial port and TCP connection handling
- Frame statistics tracking
- Hex formatting utilities
- Optional async/await support for asyncio-based applications

Basic Usage:
    from slipstream import FrameMonitor, create_connection
    
    connection = create_connection('/dev/ttyUSB0:115200')
    monitor = FrameMonitor(connection, check_crc=True)
    monitor.monitor(duration=10)
    monitor.print_stats()

Async Usage (optional):
    from slipstream.async_streaming import AsyncFrameMonitor
    from slipstream.async_connections import create_async_connection
    
    async def main():
        connection = await create_async_connection('tcp://localhost:5000')
        monitor = AsyncFrameMonitor(connection, check_crc=True)
        await monitor.monitor(duration=10)
        monitor.print_stats()

References:
    - RFC 1055: Nonstandard Transmission of IP Datagrams over Serial Lines: SLIP
    - Ethernet polynomial CRC32: 0x04C11DB7
"""

from .slip import encode_packet, decode_packet, StreamingDecoder, END, ESC, ESCEND, ESCESC
from .crc import calculate_crc32, verify_crc32, append_crc32, extract_crc32, crc32_to_hex, hex_to_crc32
from .stats import FrameStatistics
from .connections import Connection, SerialConnection, TCPConnection, create_connection
from .streaming import FrameMonitor, hexlify_frame

__version__ = "1.0.0"
__author__ = "Uli Köhler"

__all__ = [
    # SLIP encoding/decoding
    'encode_packet',
    'decode_packet',
    'StreamingDecoder',
    'END',
    'ESC',
    'ESCEND',
    'ESCESC',
    
    # CRC32
    'calculate_crc32',
    'verify_crc32',
    'append_crc32',
    'extract_crc32',
    'crc32_to_hex',
    'hex_to_crc32',
    
    # Statistics
    'FrameStatistics',
    
    # Connections
    'Connection',
    'SerialConnection',
    'TCPConnection',
    'create_connection',
    
    # High-level monitoring
    'FrameMonitor',
    'hexlify_frame',
]

# Optional async exports
try:
    from .async_slip import (
        encode_packet_async,
        decode_packet_async,
        AsyncStreamingDecoder,
        AsyncSlipCodec,
    )
    from .async_connections import (
        AsyncConnection,
        AsyncTCPConnection,
        AsyncTCPServerConnection,
        AsyncFileConnection,
        AsyncSerialConnection,
        create_async_connection,
    )
    from .async_streaming import (
        AsyncFrameMonitor,
        create_async_monitor,
    )
    
    __all__.extend([
        # Async SLIP encoding/decoding
        'encode_packet_async',
        'decode_packet_async',
        'AsyncStreamingDecoder',
        'AsyncSlipCodec',
        
        # Async connections
        'AsyncConnection',
        'AsyncTCPConnection',
        'AsyncTCPServerConnection',
        'AsyncFileConnection',
        'AsyncSerialConnection',
        'create_async_connection',
        
        # Async monitoring
        'AsyncFrameMonitor',
        'create_async_monitor',
    ])
except ImportError:
    # Async dependencies not installed, skip async exports
    pass
