#!/usr/bin/env python3
"""
SLIP Frame Monitor - Monitor and analyze SLIP frames from serial ports, TCP, UDP, or files.

Features:
- Real-time SLIP frame monitoring
- CRC32 validation (Ethernet polynomial)
- Frame statistics (count, bytes, errors)
- Hex display of frame contents
- Interactive ncurses UI mode (-i/--interactive)
- Timeout support
- Statistical reporting
- Multiple I/O backends: Serial, TCP (client/server), UDP (client/server), File

Usage:
    # Monitor serial port
    slipstream /dev/ttyUSB0
    
    # Monitor with custom baudrate
    slipstream /dev/ttyUSB0:115200
    
    # Monitor TCP connection (client)
    slipstream tcp:192.168.1.1:5000
    
    # Listen on TCP port (server)
    slipstream tcp-listen:5000
    
    # Monitor UDP connection (client)
    slipstream udp:192.168.1.1:5000
    
    # Listen on UDP port (server)
    slipstream udp-listen:5000
    
    # Monitor file
    slipstream file:/path/to/file.bin
    
    # Interactive ncurses mode
    slipstream -i /dev/ttyUSB0
    
    # Monitor for 30 seconds
    slipstream -t 30 /dev/ttyUSB0

Author: Uli Köhler
License: See LICENSE file in libSLIPStream repository
"""

import argparse
import sys
import time
import signal
import os
import logging
import json
import csv
from pathlib import Path
from datetime import datetime

# Add parent directory to path for slipstream import
sys.path.insert(0, str(Path(__file__).parent.parent))

import slipstream
from slipstream import create_connection, FrameMonitor, hexlify_frame, encode_packet


class FrameLogger:
    """Logger for SLIP frames with multiple output formats."""
    
    def __init__(self, log_file=None, log_format='text', log_valid_only=False,
                 log_timestamps=True, log_raw=False, log_append=False):
        """
        Initialize frame logger.
        
        Args:
            log_file: Path to log file (None for no file logging)
            log_format: Format for logging ('text', 'json', 'csv', 'binary')
            log_valid_only: Only log frames with valid CRC
            log_timestamps: Include timestamps in log
            log_raw: Log raw frame bytes instead of decoded
            log_append: Append to log file instead of overwriting
        """
        self.log_file = log_file
        self.log_format = log_format
        self.log_valid_only = log_valid_only
        self.log_timestamps = log_timestamps
        self.log_raw = log_raw
        self.log_append = log_append
        self.file_handle = None
        self.csv_writer = None
        self.frame_count = 0
        
        if log_file:
            self._open_log_file()
    
    def _open_log_file(self):
        """Open the log file for writing."""
        mode = 'a' if self.log_append else 'w'
        
        if self.log_format == 'binary':
            mode = mode + 'b'
        
        try:
            self.file_handle = open(self.log_file, mode)
            
            if self.log_format == 'csv':
                self.csv_writer = csv.writer(self.file_handle)
                # Write header
                header = ['frame_number', 'timestamp', 'crc_valid', 'payload_length']
                if self.log_raw:
                    header.append('raw_hex')
                else:
                    header.append('payload_hex')
                self.csv_writer.writerow(header)
                self.file_handle.flush()
        
        except Exception as e:
            print(f"Warning: Failed to open log file {self.log_file}: {e}", file=sys.stderr)
            self.file_handle = None
    
    def log_frame(self, frame_data: bytes, frame_info: dict):
        """
        Log a frame.
        
        Args:
            frame_data: Raw frame bytes
            frame_info: Frame information dict from FrameMonitor
        """
        if not self.file_handle:
            return
        
        # Check if we should log this frame
        if self.log_valid_only and frame_info.get('crc_valid') is False:
            return
        
        self.frame_count += 1
        
        timestamp = datetime.utcnow().isoformat() + 'Z' if self.log_timestamps else None
        
        try:
            if self.log_format == 'text':
                self._log_text(frame_data, frame_info, timestamp)
            elif self.log_format == 'json':
                self._log_json(frame_data, frame_info, timestamp)
            elif self.log_format == 'csv':
                self._log_csv(frame_data, frame_info, timestamp)
            elif self.log_format == 'binary':
                self._log_binary(frame_data)
        except Exception as e:
            print(f"Warning: Failed to log frame: {e}", file=sys.stderr)
    
    def _log_text(self, frame_data: bytes, frame_info: dict, timestamp: str):
        """Log frame in text format."""
        crc_status = "OK" if frame_info.get('crc_valid') else "BAD"
        data_to_log = frame_data if self.log_raw else frame_info.get('payload', b'')
        hex_str = data_to_log.hex()
        
        line_parts = []
        if timestamp:
            line_parts.append(f"[{timestamp}]")
        line_parts.append(f"Frame #{self.frame_count}")
        line_parts.append(f"CRC: {crc_status}")
        line_parts.append(f"Length: {len(data_to_log)}")
        line_parts.append(f"HEX: {hex_str}")
        
        self.file_handle.write(' '.join(line_parts) + '\n')
        self.file_handle.flush()
    
    def _log_json(self, frame_data: bytes, frame_info: dict, timestamp: str):
        """Log frame in JSON format."""
        data_to_log = frame_data if self.log_raw else frame_info.get('payload', b'')
        
        log_entry = {
            'frame_number': self.frame_count,
            'timestamp': timestamp,
            'crc_valid': frame_info.get('crc_valid'),
            'payload_length': len(data_to_log),
            'hex': data_to_log.hex()
        }
        
        if frame_info.get('crc_valid') is False:
            log_entry['crc_received'] = frame_info.get('crc_received')
            log_entry['crc_expected'] = frame_info.get('crc_expected')
        
        self.file_handle.write(json.dumps(log_entry) + '\n')
        self.file_handle.flush()
    
    def _log_csv(self, frame_data: bytes, frame_info: dict, timestamp: str):
        """Log frame in CSV format."""
        data_to_log = frame_data if self.log_raw else frame_info.get('payload', b'')
        
        row = [
            self.frame_count,
            timestamp if timestamp else '',
            frame_info.get('crc_valid'),
            len(data_to_log),
            data_to_log.hex()
        ]
        
        self.csv_writer.writerow(row)
        self.file_handle.flush()
    
    def _log_binary(self, frame_data: bytes):
        """Log frame in binary format (raw bytes)."""
        self.file_handle.write(frame_data)
        self.file_handle.flush()
    
    def close(self):
        """Close the log file."""
        if self.file_handle:
            try:
                self.file_handle.close()
            except Exception:
                pass
            self.file_handle = None
            self.csv_writer = None


class NonInteractiveMonitor:
    """Simple line-by-line frame monitor for terminal output."""
    
    def __init__(self, connection, check_crc=True, show_hex=False, show_ascii=False,
                 logger=None, follow=False):
        """
        Initialize non-interactive monitor.
        
        Args:
            connection: Connection object
            check_crc: Whether to validate CRC
            show_hex: Whether to display frame in hex
            show_ascii: Whether to display ASCII representation
            logger: FrameLogger instance for logging
            follow: Whether to follow the connection (print monitoring messages)
        """
        self.connection = connection
        self.show_hex = show_hex
        self.show_ascii = show_ascii
        self.logger = logger
        self.follow = follow
        self.monitor = FrameMonitor(connection, check_crc=check_crc)
        self.monitor.frame_callback = self._print_frame
        self.frame_count = 0
    
    def _print_frame(self, frame_data: bytes) -> None:
        """Print a received frame."""
        self.frame_count += 1
        
        last = self.monitor.get_last_frame()
        if not last:
            return
        
        # Log frame if logger is configured
        if self.logger:
            self.logger.log_frame(frame_data, last)
        
        crc_status = ""
        if last['crc_valid'] is not None:
            crc_status = " [CRC: OK]" if last['crc_valid'] else " [CRC: BAD]"
        
        print(f"\n[Frame {self.frame_count}] {len(frame_data)} bytes{crc_status}")
        
        if last['crc_valid'] is False:
            received = last.get('crc_received')
            expected = last.get('crc_expected')
            diagnostic = last.get('crc_diagnostic')
            if received is not None and expected is not None:
                print(f"  CRC received = 0x{received:08X}")
                print(f"  CRC expected = 0x{expected:08X}")
            if diagnostic:
                print(f"  CRC diagnosis = {diagnostic}")
        
        if self.show_hex:
            hex_output = hexlify_frame(frame_data)
            print(hex_output)
        elif self.show_ascii:
            try:
                ascii_str = frame_data.decode('ascii', errors='replace')
                print(f"ASCII: {ascii_str}")
            except Exception:
                pass
        else:
            # Show brief hex
            hex_str = frame_data.hex()
            if len(hex_str) > 80:
                print(f"HEX: {hex_str[:80]}...")
            else:
                print(f"HEX: {hex_str}")
    
    def run(self, duration=None):
        """
        Run the monitor.

        Args:
            duration: Duration in seconds (None for infinite)
        """
        try:
            # Only print monitoring messages if following (waiting for data)
            if self.follow:
                # Display connection info based on connection type
                if hasattr(self.connection, 'host'):
                    print(f"Monitoring {self.connection.host}:{self.connection.port}")
                elif hasattr(self.connection, 'port') and hasattr(self.connection, 'baudrate'):
                    print(f"Monitoring {self.connection.port} @ {self.connection.baudrate} baud")
                elif hasattr(self.connection, 'filepath'):
                    print(f"Monitoring file: {self.connection.filepath}")
                else:
                    print(f"Monitoring connection")

                if self.logger and self.logger.log_file:
                    print(f"Logging to {self.logger.log_file} ({self.logger.log_format} format)")
                print("Press Ctrl+C to stop...\n")

            self.monitor.monitor(duration=duration)
        except KeyboardInterrupt:
            if self.follow:
                print("\n\nStopped by user.")
        finally:
            if self.logger:
                self.logger.close()
            self.monitor.close()


class InteractiveMonitor:
    """Interactive ncurses-based monitor for real-time display and message sending."""
    
    def __init__(self, connection, check_crc=True, logger=None):
        """
        Initialize interactive monitor.
        
        Args:
            connection: Connection object
            check_crc: Whether to validate CRC
            logger: FrameLogger instance for logging
        """
        try:
            import curses
            self.curses = curses
        except ImportError:
            raise ImportError("curses library required for interactive mode")
        
        self.connection = connection
        self.check_crc = check_crc
        self.logger = logger
        self.monitor = FrameMonitor(connection, check_crc=check_crc)
        self.monitor.frame_callback = self._frame_received
        
        self.running = True
        self.display_hex = False
        self.last_frames = []
        self.max_history = 20
        
        # Input state
        self.input_buffer = ""
        self.input_mode = "text"  # "text" or "hex"
        self.show_stats = True
        self.sent_messages = []
        self.max_sent_history = 10
        
        # Signal handler for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals."""
        self.running = False
    
    def _frame_received(self, frame_data: bytes) -> None:
        """Record a received frame."""
        last = self.monitor.get_last_frame()
        if last:
            self.last_frames.append(last)
            if len(self.last_frames) > self.max_history:
                self.last_frames = self.last_frames[-self.max_history:]
            
            # Log frame if logger is configured
            if self.logger:
                self.logger.log_frame(frame_data, last)
    
    def _send_message(self, message: str) -> None:
        """Send a message through the connection."""
        try:
            if self.input_mode == "text":
                # Send as text (encode to bytes)
                data = message.encode('utf-8')
            else:
                # Send as hex (parse hex string)
                # Remove spaces and convert to bytes
                hex_str = message.replace(' ', '').replace('\n', '')
                if len(hex_str) % 2 != 0:
                    hex_str = '0' + hex_str  # Pad if odd length
                data = bytes.fromhex(hex_str)
            
            # Encode as SLIP packet
            encoded = encode_packet(data)
            
            # Send through connection
            self.connection.write(encoded)
            
            # Record sent message
            timestamp = time.time()
            self.sent_messages.append({
                'timestamp': timestamp,
                'data': data,
                'mode': self.input_mode,
                'raw': encoded
            })
            if len(self.sent_messages) > self.max_sent_history:
                self.sent_messages = self.sent_messages[-self.max_sent_history:]
            
            # Clear input buffer
            self.input_buffer = ""
            
        except Exception as e:
            # Show error in input buffer
            self.input_buffer = f"Error: {str(e)}"
    
    def _draw_header(self, stdscr, width):
        """Draw the header section."""
        stdscr.attron(self.curses.color_pair(1) | self.curses.A_BOLD)
        stdscr.addstr(0, 0, "SLIP Frame Monitor".center(width)[:width])
        stdscr.attroff(self.curses.color_pair(1) | self.curses.A_BOLD)
    
    def _draw_stats(self, stdscr, stats, row, width):
        """Draw statistics panel (if enabled)."""
        if not self.show_stats:
            return row
        
        y = row
        
        # Title
        stdscr.attron(self.curses.color_pair(2) | self.curses.A_BOLD)
        stdscr.addstr(y, 0, "Statistics".ljust(width)[:width])
        stdscr.attroff(self.curses.color_pair(2) | self.curses.A_BOLD)
        y += 1
        
        # Stats lines
        stats_lines = [
            f"  Frames:      {stats['frames_received']:6d}  |  Errors: {stats['frames_with_errors']:3d}  |  Bad CRC: {stats['frames_with_bad_crc']:3d}",
            f"  Bytes RX:    {stats['total_bytes_received']:6d}  |  Payload: {stats['total_payload_bytes']:6d}",
            f"  Rate:        {stats['frames_per_second']:6.2f} fps  |  {stats['bytes_per_second']:8.2f} bps",
            f"  Elapsed:     {stats['elapsed_seconds']:6.1f} seconds",
        ]
        
        if stats['frames_received'] > 0 and stats['min_frame_size'] is not None:
            stats_lines.append(
                f"  Frame Size:  min={stats['min_frame_size']:3d}  max={stats['max_frame_size']:3d}  "
                f"avg={stats['avg_payload_size']:6.1f}"
            )
        
        for line in stats_lines:
            if y < stdscr.getmaxyx()[0] - 1:
                stdscr.addstr(y, 0, line.ljust(width)[:width])
                y += 1
        
        return y + 1
    
    def _draw_frames(self, stdscr, row, width, height):
        """Draw recent frames and sent messages panel."""
        y = row
        max_rows = height - row - 4  # Reserve space for input area and footer
        
        if max_rows < 2:
            return y
        
        # Title
        stdscr.attron(self.curses.color_pair(2) | self.curses.A_BOLD)
        stdscr.addstr(y, 0, "Messages (RX=received, TX=sent)".ljust(width)[:width])
        stdscr.attroff(self.curses.color_pair(2) | self.curses.A_BOLD)
        y += 1
        max_rows -= 1
        
        # Combine received and sent messages, sort by timestamp
        all_messages = []
        for frame in self.last_frames:
            all_messages.append(('RX', frame))
        for msg in self.sent_messages:
            all_messages.append(('TX', msg))
        all_messages.sort(key=lambda x: x[1]['timestamp'])
        
        # Display messages in reverse order (newest first)
        for msg_type, msg in reversed(all_messages[-max_rows:]):
            if y >= height - 4:
                break
            
            if msg_type == 'RX':
                # Received frame
                crc_indicator = ""
                extra_crc = ""
                if msg['crc_valid'] is True:
                    crc_indicator = " [✓CRC]"
                elif msg['crc_valid'] is False:
                    crc_indicator = " [✗CRC]"
                    if msg.get('crc_received') is not None and msg.get('crc_expected') is not None:
                        extra_crc = f" recv={msg['crc_received']:08X} exp={msg['crc_expected']:08X}"
                
                timestamp = time.strftime('%H:%M:%S', time.localtime(msg['timestamp']))
                frame_str = (
                    f"  RX {timestamp} | {len(msg['payload']):3d} bytes | "
                    f"HEX: {msg['raw'][:20].hex().upper()}...{crc_indicator}{extra_crc}"
                )
                
                if msg['crc_valid'] is False:
                    stdscr.attron(self.curses.color_pair(3))
                
                stdscr.addstr(y, 0, frame_str.ljust(width)[:width])
                
                if msg['crc_valid'] is False:
                    stdscr.attroff(self.curses.color_pair(3))
            else:
                # Sent message
                timestamp = time.strftime('%H:%M:%S', time.localtime(msg['timestamp']))
                mode_str = msg['mode'].upper()
                frame_str = (
                    f"  TX {timestamp} | {len(msg['data']):3d} bytes | "
                    f"{mode_str}: {msg['data'][:20].hex().upper()}..."
                )
                
                stdscr.attron(self.curses.color_pair(4))
                stdscr.addstr(y, 0, frame_str.ljust(width)[:width])
                stdscr.attroff(self.curses.color_pair(4))
            
            y += 1
        
        return y + 1
    
    def _draw_input_area(self, stdscr, height, width):
        """Draw input area for sending messages."""
        y = height - 2
        
        # Input prompt
        mode_indicator = f"[{self.input_mode.upper()}]"
        prompt = f"{mode_indicator} Send: "
        
        # Draw input line
        stdscr.attron(self.curses.color_pair(1) | self.curses.A_BOLD)
        stdscr.addstr(y, 0, prompt[:width])
        stdscr.attroff(self.curses.color_pair(1) | self.curses.A_BOLD)
        
        # Draw input buffer (truncate if too long)
        max_input_len = width - len(prompt)
        if max_input_len > 0:
            input_text = self.input_buffer[-max_input_len:] if len(self.input_buffer) > max_input_len else self.input_buffer
            stdscr.addstr(y, len(prompt), input_text.ljust(max_input_len)[:max_input_len])
    
    def _draw_footer(self, stdscr, height, width):
        """Draw footer with help text."""
        y = height - 1
        footer = " q: quit  | m: toggle text/hex mode  | s: toggle stats  | c: clear  | Enter: send"
        stdscr.attron(self.curses.color_pair(1))
        stdscr.addstr(y, 0, footer.ljust(width)[:width])
        stdscr.attroff(self.curses.color_pair(1))
    
    def _main_ui(self, stdscr):
        """Main UI loop."""
        # Setup colors
        self.curses.init_pair(1, self.curses.COLOR_WHITE, self.curses.COLOR_BLUE)
        self.curses.init_pair(2, self.curses.COLOR_CYAN, self.curses.COLOR_BLACK)
        self.curses.init_pair(3, self.curses.COLOR_RED, self.curses.COLOR_BLACK)
        self.curses.init_pair(4, self.curses.COLOR_GREEN, self.curses.COLOR_BLACK)
        
        # Non-blocking input
        stdscr.nodelay(True)
        stdscr.timeout(100)
        
        # Enable special keys
        stdscr.keypad(True)
        
        last_render = 0
        
        while self.running:
            height, width = stdscr.getmaxyx()
            
            try:
                # Read from connection
                chunk = self.connection.read(timeout=0.05)
                if chunk:
                    self.monitor.process_chunk(chunk)
            except Exception:
                pass
            
            # Render UI every 100ms
            now = time.time()
            if now - last_render > 0.1:
                try:
                    stdscr.clear()
                    
                    self._draw_header(stdscr, width)
                    row = 1
                    
                    stats = self.monitor.get_stats()
                    row = self._draw_stats(stdscr, stats, row, width)
                    
                    if row < height - 3:
                        row = self._draw_frames(stdscr, row, width, height)
                    
                    self._draw_input_area(stdscr, height, width)
                    self._draw_footer(stdscr, height, width)
                    
                    stdscr.refresh()
                    last_render = now
                except self.curses.error:
                    pass
            
            # Handle input
            try:
                key = stdscr.getch()
                if key != -1:
                    if key == ord('q'):
                        self.running = False
                    elif key == ord('m'):
                        # Toggle text/hex mode
                        self.input_mode = "hex" if self.input_mode == "text" else "text"
                        self.input_buffer = ""
                    elif key == ord('s'):
                        # Toggle stats display
                        self.show_stats = not self.show_stats
                    elif key == ord('c'):
                        # Clear history
                        self.last_frames = []
                        self.sent_messages = []
                    elif key == self.curses.KEY_ENTER or key == 10 or key == 13:
                        # Send message
                        if self.input_buffer and not self.input_buffer.startswith("Error:"):
                            self._send_message(self.input_buffer)
                    elif key == self.curses.KEY_BACKSPACE or key == 127 or key == 8:
                        # Backspace
                        self.input_buffer = self.input_buffer[:-1]
                    elif key == self.curses.KEY_DC:
                        # Delete key
                        self.input_buffer = self.input_buffer[:-1]
                    elif 32 <= key <= 126:  # Printable ASCII characters
                        # Add character to input buffer
                        self.input_buffer += chr(key)
            except self.curses.error:
                pass
    
    def run(self, duration=None):
        """
        Run the interactive monitor.
        
        Args:
            duration: Duration in seconds (None for infinite)
        """
        try:
            self.curses.wrapper(self._main_ui)
        except KeyboardInterrupt:
            self.running = False
        finally:
            print("\nShutting down...")
            if self.logger:
                self.logger.close()
            self.monitor.close()


def check_ncurses_available():
    """Check if ncurses is available."""
    try:
        import curses
        return True
    except ImportError:
        return False


def create_parser() -> argparse.ArgumentParser:
    """Create and return the argument parser."""
    ncurses_available = check_ncurses_available()
    
    parser = argparse.ArgumentParser(
        description='Monitor SLIP frames from serial, TCP, UDP, or file connections',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Connection Types:
  Serial:     /dev/ttyUSB0 or /dev/ttyUSB0:115200 or serial:///dev/ttyUSB0
  TCP Client: tcp:192.168.1.1:5000 or tcp://192.168.1.1:5000
  TCP Server: tcp-listen:5000 or tcp-listen:0.0.0.0:5000
  UDP Client: udp:192.168.1.1:5000 or udp:192.168.1.1:5000:8000 (local bind port)
  UDP Server: udp-listen:5000 or udp-listen:0.0.0.0:5000
  File:       file:/path/to/file or file:/path/to/file:rb or file:///path/to/file
  Auto-detect: Path is auto-detected as file (regular file) or serial (character device)

Examples:
  # Monitor serial port (auto-detected as character device)
  slipstream /dev/ttyUSB0
  
  # Monitor with custom baudrate
  slipstream /dev/ttyUSB0:115200
  
  # Monitor serial port with explicit URL scheme
  slipstream serial:///dev/ttyUSB0
  
  # Monitor TCP connection (client)
  slipstream tcp:192.168.1.1:5000
  slipstream tcp://192.168.1.1:5000
  
  # Listen on TCP port (server)
  slipstream tcp-listen:5000
  
  # Monitor UDP connection (client)
  slipstream udp:192.168.1.1:5000
  
  # Listen on UDP port (server)
  slipstream udp-listen:5000
  
  # Monitor file (auto-detected as regular file)
  slipstream /path/to/file.bin
  
  # Monitor file with explicit URL scheme
  slipstream file:/path/to/file.bin
  slipstream file:///path/to/file.bin
''' + ('''
  # Interactive ncurses UI mode (with message sending)
  slipstream -i /dev/ttyUSB0
''' if ncurses_available else '''
  # Interactive ncurses UI mode (not available - curses module not found)
''') + '''
  # Monitor for 30 seconds
  slipstream -t 30 /dev/ttyUSB0
  
  # Show hex dump of each frame
  slipstream -x /dev/ttyUSB0
  
  # Follow file for appended data (like tail -f)
  slipstream -f /path/to/file.bin
        '''
    )
    
    parser.add_argument(
        'connection',
        help='Connection string (serial, tcp, tcp-listen, udp, udp-listen, or file)'
    )
    
    if ncurses_available:
        parser.add_argument(
            '-i', '--interactive',
            action='store_true',
            help='Use interactive ncurses UI mode (send messages, view stats, toggle modes)'
        )
    
    parser.add_argument(
        '-t', '--timeout',
        type=float,
        help='Monitor for N seconds then exit'
    )
    
    parser.add_argument(
        '-x', '--hex',
        action='store_true',
        help='Show hex dump of each frame'
    )
    
    parser.add_argument(
        '-a', '--ascii',
        action='store_true',
        help='Show ASCII representation of frames'
    )
    
    parser.add_argument(
        '--no-crc',
        action='store_true',
        help='Disable CRC32 validation'
    )
    
    # Logging options
    parser.add_argument(
        '--log-file',
        type=str,
        help='Log frames to specified file'
    )
    
    parser.add_argument(
        '--log-format',
        type=str,
        choices=['text', 'json', 'csv', 'binary'],
        default='text',
        help='Log file format (default: text)'
    )
    
    parser.add_argument(
        '--log-valid-only',
        action='store_true',
        help='Only log frames with valid CRC'
    )
    
    parser.add_argument(
        '--log-no-timestamps',
        action='store_true',
        help='Exclude timestamps from log output'
    )
    
    parser.add_argument(
        '--log-raw',
        action='store_true',
        help='Log raw frame bytes instead of decoded payload'
    )
    
    parser.add_argument(
        '--log-append',
        action='store_true',
        help='Append to log file instead of overwriting'
    )
    
    parser.add_argument(
        '-f', '--follow',
        action='store_true',
        help='For file inputs, follow appended data (like tail -f)'
    )
    
    return parser


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Check if interactive mode is requested but ncurses is not available
    if hasattr(args, 'interactive') and args.interactive and not check_ncurses_available():
        print("Error: Interactive mode requires the curses module, which is not available.", file=sys.stderr)
        print("On Windows, try using WSL or Git Bash. On Linux/macOS, curses should be available by default.", file=sys.stderr)
        return 1
    
    # Initialize logger if logging is enabled
    logger = None
    if args.log_file:
        logger = FrameLogger(
            log_file=args.log_file,
            log_format=args.log_format,
            log_valid_only=args.log_valid_only,
            log_timestamps=not args.log_no_timestamps,
            log_raw=args.log_raw,
            log_append=args.log_append
        )
    
    # Create connection
    try:
        connection = create_connection(args.connection, timeout=0.1)
        from slipstream.connections import FileConnection
        is_file = isinstance(connection, FileConnection)
        if not is_file:
            print(f"Connected to {args.connection}", file=sys.stderr)
    except Exception as e:
        print(f"Error: Failed to connect: {e}", file=sys.stderr)
        return 1
    
    # For file connections, exit immediately unless --follow is given
    if is_file and not args.follow:
        # If no explicit timeout was given, set to a small value to read the file once
        if args.timeout is None:
            args.timeout = 0.5

    # Determine if we should show monitoring messages (follow mode)
    # Follow mode for: non-file connections, file connections with --follow, or explicit timeout
    follow_mode = (not is_file) or args.follow or (args.timeout is not None and args.timeout > 0)

    try:
        # Select monitor type
        if hasattr(args, 'interactive') and args.interactive:
            monitor = InteractiveMonitor(connection, check_crc=not args.no_crc, logger=logger)
            monitor.run(duration=args.timeout)
        else:
            monitor = NonInteractiveMonitor(
                connection,
                check_crc=not args.no_crc,
                show_hex=args.hex,
                show_ascii=args.ascii,
                logger=logger,
                follow=follow_mode
            )
            monitor.run(duration=args.timeout)
        
        # Print final statistics
        connection.close()
        time.sleep(0.1)
        
        print("\n")
        monitor.monitor.print_stats()
        
        if logger:
            print(f"\nLogged {logger.frame_count} frames to {args.log_file}")
        
        return 0
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        connection.close()
        if logger:
            logger.close()
        return 1


if __name__ == '__main__':
    sys.exit(main())
