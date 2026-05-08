# slipstream - Examples

This directory contains practical examples demonstrating how to use the slipstream library.

## Examples Overview

### 1. Basic Encoding and Decoding (`01_basic_encoding.py`)

**What it shows:**
- How to encode data using SLIP
- How to decode SLIP frames
- Handling of special bytes (END and ESC)
- Processing multiple frames in a stream

**Run it:**
```bash
python3 01_basic_encoding.py
```

**Key concepts:**
- SLIP framing with special byte escaping
- Separated encoding length calculation
- Error handling for malformed frames

### 2. CRC32 Validation (`02_crc32_validation.py`)

**What it shows:**
- Calculating CRC32 checksums (Ethernet polynomial)
- Appending CRC32 to frame payload
- Extracting and verifying CRC from received frames
- Detecting corrupted frames
- Integration of CRC with SLIP encoding

**Run it:**
```bash
python3 02_crc32_validation.py
```

**Key concepts:**
- CRC32 with Ethernet polynomial (0x04C11DB7)
- Little-endian byte order for CRC storage
- CRC validation strategy
- Combining SLIP and CRC for robust frame transmission

### 3. Streaming Decoder (`03_streaming_decoder.py`)

**What it shows:**
- Using StreamingDecoder for continuous byte processing
- Handling fragmented data arriving in small chunks
- Multiple frames in a single stream
- Error recovery and statistics
- Real-world serial/TCP scenarios

**Run it:**
```bash
python3 03_streaming_decoder.py
```

**Key concepts:**
- Stateful decoding without buffering complete frames
- Callback-based frame notification
- Graceful error handling
- Frame statistics tracking

### 4. Serial Monitor Demo (`04_serial_monitor_demo.py`)

**What it shows:**
- Practical serial port monitoring (most common use case)
- Sender and receiver modes (for testing without hardware)
- Frame statistics and real-time display
- CRC validation in action
- Hex dump of received frames

**Run it:**
```bash
# Monitor mode - receive frames
python3 04_serial_monitor_demo.py --monitor /dev/ttyUSB0

# Sender mode - send test frames
python3 04_serial_monitor_demo.py --sender /dev/ttyUSB0 --duration 30

# With custom baudrate
python3 04_serial_monitor_demo.py --monitor /dev/ttyUSB0:9600

# With duration limit
python3 04_serial_monitor_demo.py --monitor /dev/ttyUSB0 --duration 60
```

**Key concepts:**
- Unified serial/TCP connection interface
- Frame statistics and metrics
- Custom callbacks for frame processing
- Error handling and graceful shutdown
- Real-world timing and data flow

### 5. TCP Echo Server (`05_tcp_echo_server.py`)

**What it shows:**
- TCP server implementation for SLIP communication
- Periodic message transmission (1Hz by default)
- Echo server that responds to incoming messages with a prefix
- Ideal for testing the interactive ncurses UI mode
- Multi-threaded handling of periodic messages and client requests

**Run it:**
```bash
# Start the server (listens on port 5000)
python3 05_tcp_echo_server.py

# Start on custom port
python3 05_tcp_echo_server.py --port 6000

# Start with 0.5 second interval
python3 05_tcp_echo_server.py --interval 0.5

# Start on specific interface
python3 05_tcp_echo_server.py --host 127.0.0.1
```

**Testing with slipstream:**
```bash
# In another terminal, connect with interactive mode
slipstream -i tcp:localhost:5000

# Or connect with non-interactive mode
slipstream tcp:localhost:5000
```

**Key concepts:**
- TCP server mode with connection handling
- Periodic message transmission for testing
- Echo server pattern for bidirectional communication
- Multi-threading for concurrent I/O operations
- Integration with interactive ncurses UI for real-time testing

## Running the Examples

### Prerequisites

```bash
# Install slipstream from the python directory
pip install -e ..

# For serial port support
pip install pyserial
```

### Examples 1-3: No Hardware Required

These examples work with simulated data and don't require any physical devices:

```bash
python3 01_basic_encoding.py
python3 02_crc32_validation.py
python3 03_streaming_decoder.py
```

### Example 4: Testing Without Hardware

To test the serial monitor example without actual serial hardware, create a virtual serial port pair:

```bash
# Terminal 1: Create virtual serial port pair
socat -d -d pty,raw,echo=0 pty,raw,echo=0
# Output will show: /dev/pts/X and /dev/pts/Y

# Terminal 2: Run sender mode on one port
python3 04_serial_monitor_demo.py --sender /dev/pts/X

# Terminal 3: Run monitor mode on the other port
python3 04_serial_monitor_demo.py --monitor /dev/pts/Y
```

### Example 4: Real Hardware

If you have a device sending SLIP frames:

```bash
# Monitor the actual serial port
python3 04_serial_monitor_demo.py --monitor /dev/ttyUSB0:115200

# Monitor for 5 minutes with statistics
python3 04_serial_monitor_demo.py --monitor /dev/ttyUSB0:115200 --duration 300
```

### Example 5: Testing Interactive Mode

The TCP echo server (Example 5) is ideal for testing the interactive ncurses UI mode without requiring serial hardware.

**Step-by-step guide:**

1. **Start the TCP echo server** (Terminal 1):
```bash
python3 05_tcp_echo_server.py
```
The server will start listening on port 5000 and display:
```
TCP Echo Server listening on 0.0.0.0:5000
Periodic message interval: 1.0 seconds
============================================================
Waiting for client connection...
```

2. **Connect with interactive mode** (Terminal 2):
```bash
slipstream -i tcp:localhost:5000
```

3. **Test the interactive UI features:**
   - **Watch incoming messages**: The server sends periodic messages every second
   - **Send a text message**: Type "Hello" and press Enter
     - The server will echo back: "Echo: Hello"
   - **Toggle to hex mode**: Press `m`
     - Type "48 65 6C 6C 6F" (hex for "Hello") and press Enter
     - The server will echo back: "Echo (hex): 48656C6C6F"
   - **Toggle stats display**: Press `s` to show/hide statistics
   - **Clear history**: Press `c` to clear message history
   - **Quit**: Press `q` to exit

4. **Test with non-interactive mode** (optional):
```bash
slipstream tcp:localhost:5000 -t 10
```
This will monitor for 10 seconds and print statistics.

**Expected output in interactive mode:**
```
┌─────────────────────────────────────────────────────┐
│              SLIP Frame Monitor                       │
├─────────────────────────────────────────────────────┤
│ Statistics                                           │
│   Frames:      5  |  Errors:   0  |  Bad CRC: 0      │
│   Bytes RX:    150  |  Payload: 120                  │
│   Rate:        5.00 fps  |  150.00 bps                │
├─────────────────────────────────────────────────────┤
│ Messages (RX=received, TX=sent)                      │
│   RX 12:34:56 | 25 bytes | HEX: C0536572766572...    │
│   TX 12:34:57 | 5 bytes | TEXT: Hello                │
│   RX 12:34:57 | 10 bytes | HEX: C04563686F3A20...    │
├─────────────────────────────────────────────────────┤
│ [TEXT] Send: Hello World_                           │
│ q: quit  | m: toggle text/hex mode  | s: toggle stats  | c: clear  | Enter: send
└─────────────────────────────────────────────────────┘
```

## Command-Line Tool

For production use, the library includes a command-line tool with ncurses UI:

```bash
# Interactive real-time monitoring
../scripts/monitor_slip.py -i /dev/ttyUSB0

# TCP monitoring
../scripts/monitor_slip.py -i tcp:192.168.1.100:5000

# With hex dump
../scripts/monitor_slip.py -x -t 60 /dev/ttyUSB0
```

## Common Patterns

### Pattern 1: Process Frames as They Arrive

```python
from slipstream import StreamingDecoder, encode_packet

def handle_frame(data):
    print(f"Got frame: {data}")

decoder = StreamingDecoder(callback=handle_frame)

# Feed data as it comes
while True:
    chunk = serial_port.read(1024)
    decoder.feed(chunk)
```

### Pattern 2: Monitor Serial Port with Statistics

```python
from slipstream import create_connection, FrameMonitor

conn = create_connection('/dev/ttyUSB0:115200')
monitor = FrameMonitor(conn, check_crc=True)

def print_frame(data):
    last = monitor.get_last_frame()
    print(f"Frame: {data.hex()}, CRC: {'✓' if last['crc_valid'] else '✗'}")

monitor.frame_callback = print_frame
monitor.monitor(duration=60)
monitor.print_stats()
```

### Pattern 3: Custom Frame Processing

```python
from slipstream import create_connection, FrameMonitor, extract_crc32, verify_crc32

conn = create_connection('/dev/ttyUSB0')
monitor = FrameMonitor(conn, check_crc=True)

def process_sensor_data(frame):
    last = monitor.get_last_frame()
    
    # Extract CRC
    payload, crc_bytes = extract_crc32(frame)
    
    if last['crc_valid']:
        # Parse application data
        sensor_id = payload[0]
        sensor_value = int.from_bytes(payload[1:3], 'big')
        print(f"Sensor {sensor_id}: {sensor_value}")

monitor.frame_callback = process_sensor_data
monitor.monitor()
```

## Troubleshooting Examples

### Issue: Module not found

```
ModuleNotFoundError: No module named 'slipstream'
```

**Solution:** Install the package first from the python directory:
```bash
pip install -e ..
```

### Issue: Serial port permission denied

```
PermissionError: [Errno 13] Permission denied: '/dev/ttyUSB0'
```

**Solution:** Add your user to the dialout group:
```bash
sudo usermod -a -G dialout $USER
# Then log out and log back in
```

### Issue: No /dev/pts devices for virtual serial ports

**Solution:** You may need to install `socat`:
```bash
sudo apt-get install socat    # Debian/Ubuntu
brew install socat            # macOS
```

## Performance Tips

1. **Use appropriate read timeout** for your use case
2. **Process frames in callbacks** to avoid blocking on I/O
3. **Enable CRC validation** only when needed for reliability
4. **Use streaming decoder** instead of buffer-based decoding for continuous data

## Further Reading

- See [../README.md](../README.md) for complete API documentation
- See [../../FramingConvention.md](../../FramingConvention.md) for SLIP frame format
- See [../../README.md](../../README.md) for C++ library documentation
