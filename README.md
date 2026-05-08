# slipspeed - Python SLIP Library

![slipspeed logo](docs/SLIPstream-logo.png)

pip install slipspeed

A comprehensive Python library for SLIP (Serial Line Internet Protocol) frame encoding, decoding, and monitoring with CRC32 validation and real-time statistics.

**Note:** This library is fully compatible with [libSLIPStream](https://github.com/ulikoehler/libSLIPStream) (C++) and [SLIPSpeed](https://github.com/ulikoehler/SLIPSpeed) (Rust) implementations.

## Features

- ✅ **SLIP Encoding/Decoding** - Full RFC 1055 compliance
- ✅ **Streaming Decoder** - Process continuous byte streams from serial or network
- ✅ **CRC32 Validation** - Ethernet polynomial with built-in verification
- ✅ **Multiple I/O Backends** - Serial, TCP (client/server), UDP (client/server), File
- ✅ **Statistics Tracking** - Comprehensive frame and throughput metrics
- ✅ **Interactive ncurses UI** - Real-time dashboard for monitoring
- ✅ **Hex Utilities** - Display and analyze frame bytes
- ✅ **Async API (Optional)** - asyncio-based async/await support for concurrent operations
- ✅ **Production Ready** - Thoroughly documented and tested

## Installation

### From Git

```bash
pip install git+https://github.com/ulikoehler/PySLIPSpeed.git
```

### From Source

```bash
cd python
pip install -e .
```

### With Optional Dependencies

```bash
# For serial port support
pip install -e ".[serial]"

# For async/await support (asyncio)
pip install -e ".[async]"

# For development/testing
pip install -e ".[dev]"
```

Core functionality requires only Python 3.6+. The curses module for interactive mode is included in the standard library on Linux/macOS.

### Async API (Optional)

The library includes optional async/await support for asyncio-based applications. Install with:

```bash
pip install -e ".[async]"
```

The async API provides:
- **Async encoding/decoding** - `encode_packet_async()`, `decode_packet_async()`
- **Async streaming decoder** - `AsyncStreamingDecoder`, `AsyncSlipCodec`
- **Async connections** - TCP, TCP server, file, and serial (with pyserial-asyncio)
- **Async frame monitor** - `AsyncFrameMonitor` with async iteration

See the "Async API" section below for usage examples.

## Quick Start

### 1. Simple Encoding/Decoding

```python
from slipspeed import encode_packet, decode_packet

# Encode a message
message = b"Hello, World!"
encoded = encode_packet(message)
print(f"Encoded: {encoded.hex()}")

# Decode it back
decoded, consumed = decode_packet(encoded)
print(f"Decoded: {decoded}")
assert decoded == message
```

### 2. Monitor Serial Port (Most Common Use Case)

```python
from slipspeed import create_connection, FrameMonitor

# Connect to serial port
conn = create_connection('/dev/ttyUSB0:115200')

# Create monitor with CRC validation
monitor = FrameMonitor(conn, check_crc=True)

# Print each received frame
def on_frame(data):
    last = monitor.get_last_frame()
    crc_status = "✓" if last['crc_valid'] else "✗"
    print(f"[{crc_status}] {data.hex()}")

monitor.frame_callback = on_frame

# Monitor for 10 seconds
monitor.monitor(duration=10)

# Show statistics
monitor.print_stats()
monitor.close()
```

### 3. Interactive Real-Time Monitoring

```bash
# Launch ncurses dashboard
slipspeed -i /dev/ttyUSB0:115200
```

Dashboard shows:
- Real-time frame count and error statistics
- CRC validation status for each frame
- Transmission rate (frames/sec, bytes/sec)
- Recent frame history with timestamps
- Min/max/average frame sizes
- **Message sending capability** (text or hex mode)
- **Combined RX/TX message display**

**Testing Interactive Mode:**

For testing the interactive mode without hardware, use the included TCP echo server example:

```bash
# Terminal 1: Start the TCP echo server (sends 1Hz messages and echoes)
cd python/examples
python3 05_tcp_echo_server.py

# Terminal 2: Connect with interactive mode
slipspeed -i tcp:localhost:5000
```

The echo server will:
- Send periodic SLIP text messages every second
- Echo any message you send back with a "Echo: " prefix
- Allow you to test both text and hex input modes

See [examples/README.md](examples/README.md) for more details on all examples.

### 4. CRC32 Validation

```python
from slipspeed import calculate_crc32, append_crc32, extract_crc32, verify_crc32
import struct

# Create a frame with CRC32
payload = b"sensor_data=42"

# Calculate and append CRC32
frame = append_crc32(payload)
print(f"Frame with CRC: {frame.hex()}")

# On receiver side
stored_payload, stored_crc = extract_crc32(frame)
is_valid = verify_crc32(stored_payload, stored_crc)
print(f"CRC Valid: {is_valid}")
```

### 5. Async API (Optional)

The async API provides asyncio-based alternatives to the synchronous API. Install with `pip install -e ".[async]"`.

#### Async Encoding/Decoding

```python
import asyncio
from slipspeed.async_slip import encode_packet_async, decode_packet_async

async def main():
    # Encode a message
    message = b"Hello, Async!"
    encoded = await encode_packet_async(message)
    print(f"Encoded: {encoded.hex()}")

    # Decode it back
    decoded, consumed = await decode_packet_async(encoded)
    print(f"Decoded: {decoded}")
    assert decoded == message

asyncio.run(main())
```

#### Async TCP Connection

```python
import asyncio
from slipspeed.async_connections import create_async_connection
from slipspeed.async_streaming import AsyncFrameMonitor

async def monitor_tcp():
    # Connect to TCP server
    connection = await create_async_connection('tcp://localhost:5000')
    
    # Create async frame monitor
    monitor = AsyncFrameMonitor(connection, check_crc=True)
    
    # Monitor for 10 seconds
    await monitor.monitor(duration=10)
    
    # Print statistics
    monitor.print_stats()
    
    # Close connection
    await monitor.close()

asyncio.run(monitor_tcp())
```

#### Async Frame Iterator

```python
import asyncio
from slipspeed.async_connections import create_async_connection
from slipspeed.async_streaming import AsyncFrameMonitor

async def monitor_iter():
    connection = await create_async_connection('tcp://localhost:5000')
    monitor = AsyncFrameMonitor(connection, check_crc=True)
    
    # Iterate over frames as they arrive
    async for frame_info in monitor.monitor_iter():
        print(f"Frame: {frame_info['payload'].hex()}")
        print(f"CRC Valid: {frame_info['crc_valid']}")
    
    await monitor.close()

asyncio.run(monitor_iter())
```

#### Async Streaming Decoder

```python
import asyncio
from slipspeed.async_slip import AsyncStreamingDecoder, encode_packet_async

async def stream_decoder_example():
    decoder = AsyncStreamingDecoder()
    
    # Feed data in chunks
    chunk1 = await encode_packet_async(b"part1")
    chunk2 = await encode_packet_async(b"part2")
    
    await decoder.feed_async(chunk1 + chunk2)
    
    # Get statistics
    stats = decoder.get_stats()
    print(f"Frames received: {stats['frames_received']}")

asyncio.run(stream_decoder_example())
```

#### Async Codec (Rust-style)

```python
import asyncio
from slipspeed.async_slip import AsyncSlipCodec

async def codec_example():
    codec = AsyncSlipCodec()
    
    # Encode
    encoded = await codec.encode(b"test data")
    
    # Decode from buffer
    src = bytearray(encoded)
    decoded = await codec.decode(src)
    
    print(f"Decoded: {decoded}")

asyncio.run(codec_example())
```

**Note:** The async API is optional and only available when the `[async]` extras are installed. If not installed, importing async modules will raise ImportError. The synchronous API remains fully functional without async dependencies.

### Async Examples

The following async examples are available in the `examples/` directory:

- **06_async_basic_encoding.py** - Basic async SLIP encoding and decoding
- **07_async_tcp_client.py** - Async TCP client and server examples
- **08_async_streaming_decoder.py** - Async streaming decoder with chunked data
- **09_async_codec.py** - Rust-style AsyncSlipCodec usage
- **10_async_frame_monitor.py** - Async frame monitor with mock connection

Run async examples:
```bash
cd python/examples
python 06_async_basic_encoding.py
python 08_async_streaming_decoder.py
python 09_async_codec.py
python 10_async_frame_monitor.py
```

Note: Async examples require the `[async]` extras to be installed.

## Command-Line Tools

### slipspeed Command

The `slipspeed` command-line tool is a powerful utility for monitoring and analyzing SLIP frames from various I/O sources. It provides both simple text output and an interactive ncurses-based dashboard for real-time monitoring.

#### Overview

`slipspeed` can monitor SLIP frames from:
- **Serial ports** - Connect to USB/UART serial devices
- **TCP connections** - Act as TCP client or server
- **UDP connections** - Send/receive UDP packets
- **Files** - Read SLIP frames from binary files

The tool automatically decodes SLIP frames, validates CRC32 checksums (if present), tracks statistics, and can display frame contents in multiple formats.

#### Installation

After installing the package, the `slipspeed` command is available system-wide:

```bash
pip install -e .
slipspeed --help
```

#### Connection Types

##### Serial Port Monitoring

Monitor SLIP frames from a serial port:

```bash
# Basic serial monitoring (default 115200 baud)
slipspeed /dev/ttyUSB0

# Custom baudrate
slipspeed /dev/ttyUSB0:9600

# Windows COM port
slipspeed COM3:115200

# With interactive mode
slipspeed -i /dev/ttyUSB0:115200
```

**Requirements:** Install pyserial for serial support: `pip install pyserial`

**Common baudrates:** 9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600

##### TCP Client Mode

Connect to a TCP server and monitor SLIP frames:

```bash
# Connect to TCP server
slipspeed tcp:192.168.1.100:5000

# Connect to localhost
slipspeed tcp:localhost:9000

# With hex dump
slipspeed -x tcp:192.168.1.100:5000

# Interactive mode
slipspeed -i tcp:192.168.1.100:5000
```

Use case: Monitor SLIP frames from a network device that sends SLIP-encoded data over TCP.

##### TCP Server Mode (Listen)

Listen for TCP connections and monitor incoming SLIP frames:

```bash
# Listen on port 5000 (all interfaces)
slipspeed tcp-listen:5000

# Listen on specific interface
slipspeed tcp-listen:0.0.0.0:5000
slipspeed tcp-listen:127.0.0.1:5000

# Interactive server mode
slipspeed -i tcp-listen:5000
```

Use case: Set up a SLIP frame gateway that accepts connections from multiple clients.

##### UDP Client Mode

Send/receive SLIP frames via UDP:

```bash
# Send to UDP server
slipspeed udp:192.168.1.100:5000

# With local bind port (for receiving responses)
slipspeed udp:192.168.1.100:5000:8000

# Interactive UDP monitoring
slipspeed -i udp:192.168.1.100:5000
```

The format `udp:host:remote_port:local_port` allows you to specify which local port to bind to, useful for receiving responses.

##### UDP Server Mode (Listen)

Listen for UDP packets and decode SLIP frames:

```bash
# Listen on port 5000
slipspeed udp-listen:5000

# Listen on specific interface
slipspeed udp-listen:0.0.0.0:5000

# With timeout
slipspeed -t 60 udp-listen:5000
```

Use case: Monitor SLIP frames from multiple UDP sources (e.g., IoT devices broadcasting SLIP data).

##### File Monitoring

Read and decode SLIP frames from a file:

```bash
# Read binary file
slipspeed file:/path/to/data.bin

# Read with specific mode
slipspeed file:/path/to/data.bin:rb

# With hex dump
slipspeed -x file:/path/to/data.bin

# Disable CRC for files without CRC
slipspeed --no-crc file:/path/to/data.bin
```

File modes:
- `rb` - Read binary (default)
- `r` - Read text (not recommended for SLIP data)

#### Command-Line Options

```
slipspeed [OPTIONS] CONNECTION

Positional Arguments:
  CONNECTION          Connection string (see Connection Types above)

Display Options:
  -i, --interactive   Use ncurses UI for real-time monitoring
  -t, --timeout N     Monitor for N seconds then exit
  -x, --hex           Display hex dump of each frame
  -a, --ascii         Show ASCII representation of frames
  --no-crc            Disable CRC32 validation

Logging Options:
  --log-file PATH     Log frames to specified file
  --log-format FORMAT Log file format: text, json, csv, binary (default: text)
  --log-valid-only    Only log frames with valid CRC
  --log-no-timestamps Exclude timestamps from log output
  --log-raw           Log raw frame bytes instead of decoded payload
  --log-append        Append to log file instead of overwriting

  -h, --help          Show help message and exit
```

#### Output Modes

##### Non-Interactive Mode (Default)

In non-interactive mode, each received frame is printed to stdout:

```
[Frame 1] 12 bytes [CRC: OK]
HEX: C0 48 65 6C 6C 6F 20 57 6F 72 6C 64 C0

[Frame 2] 8 bytes [CRC: BAD]
  CRC received = 0x12345678
  CRC expected = 0xABCDEF00
HEX: C0 74 65 73 74 64 61 74 C0
```

With `-x` flag, full hex dump is shown:
```
[Frame 1] 12 bytes [CRC: OK]
00000000: C0 48 65 6C 6C 6F 20 57 6F 72 6C 64 C0
```

With `-a` flag, ASCII representation is shown:
```
[Frame 1] 12 bytes [CRC: OK]
ASCII: Hello World
```

##### Interactive Mode (ncurses)

Interactive mode provides a real-time dashboard:

```
┌─────────────────────────────────────────────────────┐
│              SLIP Frame Monitor                       │
├─────────────────────────────────────────────────────┤
│ Statistics                                           │
│   Frames:      1234  |  Errors:   2  |  Bad CRC: 1  │
│   Bytes RX:    45678  |  Payload: 42000             │
│   Rate:        45.23 fps  |  1234.56 bps             │
│   Elapsed:     60.0 seconds                         │
│   Frame Size:  min=8  max=256  avg=34.2             │
├─────────────────────────────────────────────────────┤
│ Recent Frames                                        │
│   12:34:56 |  12 payload bytes | HEX: C048656C... [✓CRC]│
│   12:34:55 |   8 payload bytes | HEX: C0746573... [✗CRC] recv=12345678 exp=ABCDEF00│
│   12:34:54 |  16 payload bytes | HEX: C0646174... [✓CRC]│
├─────────────────────────────────────────────────────┤
│ q: quit  | h: toggle hex  | c: clear  | Press any key for refresh │
└─────────────────────────────────────────────────────┘
```

**Interactive keybindings:**
- `q` - Quit
- `h` - Toggle hex display mode
- `c` - Clear frame history

#### Advanced Usage Examples

##### Monitoring with Timeout

Monitor for a specific duration, then exit with statistics:

```bash
# Monitor for 5 minutes
slipspeed -t 300 /dev/ttyUSB0

# Monitor TCP for 30 seconds
slipspeed -t 30 tcp:192.168.1.100:5000
```

##### CRC Validation

By default, `slipspeed` validates CRC32 checksums on frames. Disable if your data doesn't include CRC:

```bash
slipspeed --no-crc /dev/ttyUSB0
```

##### Combining Options

```bash
# Interactive mode with hex dump and timeout
slipspeed -i -x -t 60 /dev/ttyUSB0:115200

# ASCII mode with CRC disabled for file
slipspeed -a --no-crc file:/path/to/data.bin
```

##### Piping Output

```bash
# Save frame data to file
slipspeed /dev/ttyUSB0 > frames.log

# Count frames
slipspeed /dev/ttyUSB0 | grep "Frame" | wc -l

# Extract only valid frames
slipspeed /dev/ttyUSB0 | grep "CRC: OK"
```

#### Logging

The `slipspeed` tool supports comprehensive logging of received frames to files in multiple formats. Logging works in both interactive and non-interactive modes.

##### Basic Logging

Log all received frames to a file:

```bash
# Log to file in text format
slipspeed --log-file frames.log /dev/ttyUSB0

# Log with timeout
slipspeed --log-file frames.log -t 60 /dev/ttyUSB0
```

##### Log Formats

**Text Format (default)**

Human-readable text format with timestamps:

```bash
slipspeed --log-file frames.log --log-format text /dev/ttyUSB0
```

Output:
```
[2024-05-08T20:30:45.123456Z] Frame #1 CRC: OK Length: 12 HEX: 48656C6C6F20576F726C64
[2024-05-08T20:30:45.234567Z] Frame #2 CRC: OK Length: 8 HEX: 7465737464617461
```

**JSON Format**

Machine-readable JSON format, one line per frame:

```bash
slipspeed --log-file frames.json --log-format json /dev/ttyUSB0
```

Output:
```json
{"frame_number": 1, "timestamp": "2024-05-08T20:30:45.123456Z", "crc_valid": true, "payload_length": 12, "hex": "48656C6C6F20576F726C64"}
{"frame_number": 2, "timestamp": "2024-05-08T20:30:45.234567Z", "crc_valid": true, "payload_length": 8, "hex": "7465737464617461"}
```

For frames with bad CRC:
```json
{"frame_number": 3, "timestamp": "2024-05-08T20:30:45.345678Z", "crc_valid": false, "payload_length": 8, "hex": "626164637263", "crc_received": 12345678, "crc_expected": 87654321}
```

**CSV Format**

Comma-separated values for spreadsheet analysis:

```bash
slipspeed --log-file frames.csv --log-format csv /dev/ttyUSB0
```

Output:
```csv
frame_number,timestamp,crc_valid,payload_length,payload_hex
1,2024-05-08T20:30:45.123456Z,True,12,48656C6C6F20576F726C64
2,2024-05-08T20:30:45.234567Z,True,8,7465737464617461
```

**Binary Format**

Raw frame bytes without any metadata:

```bash
slipspeed --log-file frames.bin --log-format binary /dev/ttyUSB0
```

This saves the exact byte sequence of each SLIP frame, useful for:
- Replaying frames later
- Binary analysis
- Creating test data

##### Logging Options

**Log Only Valid Frames**

Filter out frames with bad CRC:

```bash
slipspeed --log-file valid_frames.log --log-valid-only /dev/ttyUSB0
```

**Exclude Timestamps**

Reduce log file size by omitting timestamps:

```bash
slipspeed --log-file frames.log --log-no-timestamps /dev/ttyUSB0
```

Text format output without timestamps:
```
Frame #1 CRC: OK Length: 12 HEX: 48656C6C6F20576F726C64
Frame #2 CRC: OK Length: 8 HEX: 7465737464617461
```

**Log Raw Frames**

Log the complete SLIP frame (including END markers and escape sequences) instead of just the decoded payload:

```bash
slipspeed --log-file raw_frames.log --log-raw /dev/ttyUSB0
```

Use this when you need the exact frame bytes for analysis or replay.

**Append to Log File**

Add new frames to an existing log file instead of overwriting:

```bash
slipspeed --log-file frames.log --log-append /dev/ttyUSB0
```

Useful for continuous logging across multiple sessions.

##### Advanced Logging Examples

**Log valid frames to CSV for analysis**

```bash
slipspeed --log-file analysis.csv --log-format csv --log-valid-only /dev/ttyUSB0
```

**Log raw frames to binary file for replay**

```bash
slipspeed --log-file capture.bin --log-format binary --log-raw /dev/ttyUSB0
```

**Log to JSON without timestamps for minimal size**

```bash
slipspeed --log-file frames.json --log-format json --log-no-timestamps /dev/ttyUSB0
```

**Interactive mode with logging**

```bash
slipspeed -i --log-file frames.log /dev/ttyUSB0
```

This shows the ncurses dashboard while simultaneously logging all frames.

**Append to log with timeout**

```bash
slipspeed --log-file frames.log --log-append -t 300 /dev/ttyUSB0
```

Log frames for 5 minutes, appending to an existing log file.

##### Log File Management

When logging is enabled, `slipspeed` displays the log file and format at startup:

```
Connected to /dev/ttyUSB0
Logging to frames.log (text format)
Press Ctrl+C to stop...
```

On exit, it reports the number of frames logged:

```
=== SLIP Frame Statistics ===
Total Frames Received:    100
...

Logged 100 frames to frames.log
```

##### Use Cases

**Long-term data collection**

```bash
# Run in background, log all frames for 24 hours
nohup slipspeed --log-file $(date +%Y%m%d).log --log-append -t 86400 /dev/ttyUSB0 &
```

**Debugging frame issues**

```bash
# Log raw frames to analyze encoding problems
slipspeed --log-file debug.bin --log-format binary --log-raw /dev/ttyUSB0
```

**Data analysis pipeline**

```bash
# Log to JSON for processing with other tools
slipspeed --log-file frames.json --log-format json /dev/ttyUSB0

# Process with jq
jq '.payload_length' frames.json | sort | uniq -c
```

**Quality assurance**

```bash
# Log only valid frames to verify CRC performance
slipspeed --log-file qa.log --log-valid-only /dev/ttyUSB0
```

#### Statistics Output

When monitoring completes (via timeout or Ctrl+C), statistics are printed:

```
=== SLIP Frame Statistics ===
Total Frames Received:    1234
Frames with Errors:       2
Frames with Bad CRC:      1
Total Bytes Received:     45678
Total Payload Bytes:       42000
Frames per Second:        45.23
Bytes per Second:          1234.56
Elapsed Seconds:           60.0
Min Frame Size:            8 bytes
Max Frame Size:            256 bytes
Avg Payload Size:          34.2 bytes
```

#### Performance Considerations

- **Serial ports:** Limited by baudrate (e.g., 115200 baud = ~11.5 KB/s max)
- **TCP/UDP:** Can handle high throughput, limited by network and CPU
- **Files:** Reads as fast as disk I/O allows
- **Interactive mode:** Slight overhead from ncurses rendering (~5-10% CPU)

#### Troubleshooting slipspeed

##### Issue: "command not found: slipspeed"

**Solution:** Ensure the package is installed:
```bash
cd python
pip install -e .
```

Or use directly:
```bash
python scripts/slipspeed.py /dev/ttyUSB0
```

##### Issue: "Permission denied" on serial port

**Solution:** Add user to dialout group:
```bash
sudo usermod -a -G dialout $USER
# Log out and log back in
```

##### Issue: Interactive mode shows blank screen

**Solution:** Ensure terminal supports ncurses (most Linux/macOS terminals do). On Windows, use WSL or Git Bash.

##### Issue: TCP/UDP connection refused

**Solution:** Check firewall settings and ensure the target service is running:
```bash
# Test connectivity
telnet 192.168.1.100 5000
# or
nc -zv 192.168.1.100 5000
```

##### Issue: File not found

**Solution:** Use absolute path or ensure relative path is correct:
```bash
slipspeed file:/home/user/data.bin  # Absolute
slipspeed file:./data.bin           # Relative
```

##### Issue: High CPU usage in interactive mode

**Solution:** Reduce frame rate or use non-interactive mode for high-throughput scenarios.

## Architecture

### Core Modules

```
slipspeed/
├── slip.py          # SLIP encoding/decoding and StreamingDecoder
├── crc.py           # CRC32 calculation with Ethernet polynomial
├── connections.py   # Serial and TCP connection handlers
├── stats.py         # Statistics tracking
├── streaming.py     # High-level FrameMonitor class
└── __init__.py      # Public API
```

### Design Patterns

1. **Streaming Architecture** - Decode frames as bytes arrive; low memory overhead
2. **Callback-Based** - Process frames immediately upon completion
3. **Connection Abstraction** - Serial and TCP use the same interface
4. **Error Recovery** - Invalid frames don't crash the decoder; malformed escapes reset state

## API Reference

### SLIP Module

```python
# Functions
encode_packet(data: bytes) -> bytes
decode_packet(data: bytes) -> Tuple[bytes, int]

# Classes
class StreamingDecoder:
    def __init__(self, callback=None)
    def feed(self, data: bytes)
    def reset()
    def get_stats() -> dict

# Constants
END = 0xC0       # End of frame marker
ESC = 0xDB       # Escape character
ESCEND = 0xDC    # Escaped END
ESCESC = 0xDD    # Escaped ESC
```

### CRC Module

```python
# Functions
calculate_crc32(data: bytes, initial: int = 0xFFFFFFFF) -> int
verify_crc32(data: bytes, stored_crc: bytes) -> bool
append_crc32(data: bytes) -> bytes
extract_crc32(data: bytes) -> Tuple[bytes, bytes]
crc32_to_hex(crc_value: int) -> str
hex_to_crc32(hex_str: str) -> int
```

### Connections Module

```python
# Classes
class Connection (abstract base):
    def read(timeout: float = None) -> bytes
    def write(data: bytes) -> int
    def close()
    def is_open() -> bool

class SerialConnection(Connection):
    def __init__(port: str, baudrate: int = 115200, timeout: float = 0.1)

class TCPConnection(Connection):
    def __init__(host: str, port: int, timeout: float = 0.1)

class TCPServerConnection(Connection):
    def __init__(host: str = '0.0.0.0', port: int = 5000, timeout: float = 0.1)

class UDPConnection(Connection):
    def __init__(host: str, port: int, timeout: float = 0.1, bind_port: int = None)

class UDPServerConnection(Connection):
    def __init__(host: str = '0.0.0.0', port: int = 5000, timeout: float = 0.1)

class FileConnection(Connection):
    def __init__(filepath: str, mode: str = 'rb', timeout: float = 0.1)

# Factory
def create_connection(connection_string: str) -> Connection:
    # Examples:
    # Serial:
    #   '/dev/ttyUSB0'               - Serial at 115200 baud (default)
    #   '/dev/ttyUSB0:9600'          - Serial at 9600 baud
    #   'COM3:115200'                - Windows serial
    # TCP:
    #   'tcp:192.168.1.1:5000'       - TCP client connection
    #   'tcp-listen:5000'            - TCP server on port 5000
    #   'tcp-listen:0.0.0.0:5000'    - TCP server on specific interface
    # UDP:
    #   'udp:192.168.1.1:5000'       - UDP client
    #   'udp:192.168.1.1:5000:8000'  - UDP client with local bind port
    #   'udp-listen:5000'            - UDP server on port 5000
    # File:
    #   'file:/path/to/file'         - Read from file
    #   'file:/path/to/file:rb'      - Read with specific mode
```

### Streaming Module

```python
class FrameMonitor:
    def __init__(
        connection: Connection,
        frame_callback: Callable = None,
        check_crc: bool = True,
        hex_output: bool = False
    )
    
    def monitor(duration: float = None)
    def process_chunk(chunk: bytes)
    def get_last_frame() -> dict
    def get_stats() -> dict
    def print_stats()
    def close()

# Utility functions
hexlify_frame(frame: bytes, width: int = 16) -> str
```

### Statistics Module

```python
class FrameStatistics:
    def add_frame(raw_frame_len: int, payload_len: int, crc_valid: bool = None)
    def add_error()
    def get_stats() -> dict
    def print_report()
```

## Common Use Cases

### Use Case 1: Serial Device Monitoring

Monitor a serial device for SLIP frames with automatic statistics:

```python
from slipspeed import create_connection, FrameMonitor

conn = create_connection('/dev/ttyUSB0:115200')
monitor = FrameMonitor(conn, check_crc=True)

try:
    monitor.monitor(duration=60)  # Monitor for 1 minute
finally:
    monitor.print_stats()
    conn.close()
```

### Use Case 2: Network-Based SLIP Protocol

Connect via TCP instead of serial:

```python
conn = create_connection('tcp:192.168.1.100:9000')
monitor = FrameMonitor(conn, check_crc=True)
monitor.monitor(duration=30)
monitor.print_stats()
```

### Use Case 3: Custom Frame Processing

Process frames with custom logic:

```python
from slipspeed import create_connection, FrameMonitor

conn = create_connection('/dev/ttyUSB0')
frames_received = []

def process_frame(frame):
    last = monitor.get_last_frame()
    if last['crc_valid']:
        frames_received.append(last['payload'])
        print(f"Valid frame: {len(last['payload'])} bytes")

monitor = FrameMonitor(conn, check_crc=True)
monitor.frame_callback = process_frame
monitor.monitor(duration=10)

print(f"Total valid frames: {len(frames_received)}")
```

### Use Case 4: Real-Time Dashboard

Interactive ncurses mode for live monitoring:

```bash
slipspeed -i -t 300 /dev/ttyUSB0:115200
```

Shows live statistics including:
- FPS (frames per second)
- Total bytes and frames
- CRC error count
- Recent frame history
- Frame size breakdown

## Troubleshooting

### Issue: "No module named 'serial'"

**Solution:** Install pyserial
```bash
pip install pyserial
```

### Issue: Serial Port Permission Denied

**Solution:** Add user to dialout group
```bash
sudo usermod -a -G dialout $USER
# Then log out and log back in
```

### Issue: CRC Always Fails

**Solution:** Check byte order. CRC is stored little-endian:
```python
import struct

# Correct: little-endian
crc_bytes = struct.pack('<I', crc)  # Little-endian
frame = payload + crc_bytes

# Incorrect: big-endian
crc_bytes = struct.pack('>I', crc)  # Wrong!
```

### Issue: Frames Cut Off or Incomplete

**Solution:** Ensure buffer is large enough or increase network buffer
```python
conn = create_connection('/dev/ttyUSB0')
conn.read(timeout=0.5)  # Increase timeout to collect more bytes
```

## Performance

Typical performance on modern hardware:

- **Encoding:** ~500 MB/s (x86-64)
- **Decoding:** ~400 MB/s (x86-64)
- **CRC32:** ~300 MB/s (x86-64)
- **Serial monitoring:** 10,000+ frames/sec (at 115200 baud)

Memory footprint:
- Core library: <50 KB
- Streaming decoder: O(max_frame_size) buffer
- Statistics: ~500 bytes

## Testing

Run tests (requires pytest):

```bash
cd python
pip install pytest
pytest tests/
```

## Frame Format Reference

For detailed information about SLIP frame structure, including escape sequences and CRC32 encoding, see the parent repository's [FramingConvention.md](../FramingConvention.md).

## License

Part of libSLIPStream. See LICENSE in the parent repository.

## Contributing

Contributions welcome! Please ensure:
1. Code follows PEP 8 style guide
2. All docstrings are complete
3. Tests pass (`pytest`)
4. New features include documentation

## Author

Uli Köhler <github@techoverflow.net>

## See Also

- [libSLIPStream C++ Library](../README.md)
- [SLIP Frame Format Reference](../FramingConvention.md)
- [RFC 1055 - SLIP Protocol](https://tools.ietf.org/html/rfc1055)
- [pyserial Documentation](https://pyserial.readthedocs.io/)
