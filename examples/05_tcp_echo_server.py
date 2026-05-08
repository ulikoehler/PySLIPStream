#!/usr/bin/env python3
"""
Example 5: TCP Echo Server with Periodic Messages

This example demonstrates a TCP server that:
1. Listens for TCP connections
2. Periodically (1Hz) transmits SLIP-encoded text messages
3. Echoes any incoming SLIP messages with a prefix

This is useful for testing the interactive ncurses UI mode of slipstream.
You can run this server and then connect to it using:
  slipstream -i tcp:localhost:5000

The server will send periodic messages and echo back anything you send.

Usage:
    # Start the server (listens on port 5000)
    python3 05_tcp_echo_server.py

    # In another terminal, connect with interactive mode
    slipstream -i tcp:localhost:5000

    # Or connect with non-interactive mode
    slipstream tcp:localhost:5000
"""

import sys
import time
import threading
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slipspeed import (
    create_connection,
    encode_packet,
    decode_packet
)


class TCPEchoServer:
    """TCP server that sends periodic messages and echoes incoming messages."""
    
    def __init__(self, host='0.0.0.0', port=5000, interval=1.0):
        """
        Initialize the TCP echo server.
        
        Args:
            host: Host to bind to (default: 0.0.0.0 for all interfaces)
            port: Port to listen on (default: 5000)
            interval: Interval in seconds for periodic messages (default: 1.0)
        """
        self.host = host
        self.port = port
        self.interval = interval
        self.running = False
        self.connection = None
        self.client_thread = None
        self.periodic_thread = None
        self.message_counter = 0
    
    def _send_periodic_message(self):
        """Send periodic SLIP messages at 1Hz."""
        while self.running and self.connection:
            try:
                # Create a message with timestamp
                message = f"Server message #{self.message_counter} at {time.strftime('%H:%M:%S')}"
                self.message_counter += 1
                
                # Encode as SLIP packet
                encoded = encode_packet(message.encode('utf-8'))
                
                # Send the message
                self.connection.write(encoded)
                print(f"[TX] Sent: {message}")
                
            except Exception as e:
                print(f"[Error] Failed to send periodic message: {e}")
                break
            
            # Wait for the interval
            time.sleep(self.interval)
    
    def _handle_client(self):
        """Handle incoming messages from the client and echo them back."""
        while self.running and self.connection:
            try:
                # Read data from connection
                data = self.connection.read(timeout=0.1)
                
                if data:
                    # Try to decode as SLIP packet
                    try:
                        decoded, consumed = decode_packet(data)
                        
                        # Decode the payload as text
                        try:
                            text = decoded.decode('utf-8', errors='replace')
                            print(f"[RX] Received: {text}")
                            
                            # Echo back with prefix
                            echo_message = f"Echo: {text}"
                            echo_encoded = encode_packet(echo_message.encode('utf-8'))
                            self.connection.write(echo_encoded)
                            print(f"[TX] Echoed: {echo_message}")
                            
                        except UnicodeDecodeError:
                            # Not text, echo as hex
                            hex_str = decoded.hex().upper()
                            echo_message = f"Echo (hex): {hex_str}"
                            echo_encoded = encode_packet(echo_message.encode('utf-8'))
                            self.connection.write(echo_encoded)
                            print(f"[TX] Echoed: {echo_message}")
                    
                    except Exception as decode_error:
                        # Not a valid SLIP packet, ignore
                        pass
                
            except Exception as e:
                if self.running:
                    print(f"[Error] Failed to read from client: {e}")
                break
    
    def start(self):
        """Start the TCP echo server."""
        try:
            # Create TCP server connection
            conn_string = f"tcp-listen:{self.host}:{self.port}"
            self.connection = create_connection(conn_string)
            
            print(f"TCP Echo Server listening on {self.host}:{self.port}")
            print(f"Periodic message interval: {self.interval} seconds")
            print("=" * 60)
            print("Waiting for client connection...")
            
            # Wait for a client to connect
            # Note: This will block until a client connects
            self.running = True
            
            # Start periodic message thread
            self.periodic_thread = threading.Thread(
                target=self._send_periodic_message,
                daemon=True
            )
            self.periodic_thread.start()
            
            # Start client handler thread
            self.client_thread = threading.Thread(
                target=self._handle_client,
                daemon=True
            )
            self.client_thread.start()
            
            # Wait for threads to complete (or until interrupted)
            try:
                while self.running:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                print("\nShutting down...")
                self.running = False
            
            # Wait for threads to finish
            if self.periodic_thread:
                self.periodic_thread.join(timeout=2)
            if self.client_thread:
                self.client_thread.join(timeout=2)
            
            print(f"\nServer stopped. Sent {self.message_counter} periodic messages.")
            
        except Exception as e:
            print(f"Error: Failed to start server: {e}")
            return 1
        finally:
            if self.connection:
                self.connection.close()
        
        return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='TCP Echo Server with Periodic SLIP Messages',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Start server on default port 5000
  python3 05_tcp_echo_server.py
  
  # Start server on custom port
  python3 05_tcp_echo_server.py --port 6000
  
  # Start server with 0.5 second interval
  python3 05_tcp_echo_server.py --interval 0.5
  
  # Start server on specific interface
  python3 05_tcp_echo_server.py --host 127.0.0.1

Testing with slipstream:
  # In another terminal, connect with interactive mode
  slipstream -i tcp:localhost:5000
  
  # Or connect with non-interactive mode
  slipstream tcp:localhost:5000
        '''
    )
    
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Host to bind to (default: 0.0.0.0 for all interfaces)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port to listen on (default: 5000)'
    )
    
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval in seconds for periodic messages (default: 1.0)'
    )
    
    args = parser.parse_args()
    
    server = TCPEchoServer(
        host=args.host,
        port=args.port,
        interval=args.interval
    )
    
    return server.start()


if __name__ == '__main__':
    sys.exit(main())
