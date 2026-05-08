#!/usr/bin/env python3
"""
Example 7: Async TCP Client with SLIP

This example shows how to use async SLIP with TCP connections.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slipspeed.async_connections import create_async_connection
from slipspeed.async_streaming import AsyncFrameMonitor


async def tcp_client_example(host='localhost', port=5000):
    """Demonstrate async TCP client with SLIP."""
    
    print("=" * 60)
    print("Async TCP Client with SLIP")
    print("=" * 60)
    print(f"Connecting to {host}:{port}...")
    
    try:
        # Create async TCP connection
        connection = await create_async_connection(f'tcp://{host}:{port}')
        print("Connected!")
        
        # Create frame monitor
        monitor = AsyncFrameMonitor(connection, check_crc=False)
        
        # Send a test frame
        test_data = b"Hello from async TCP client!"
        print(f"\nSending frame: {test_data}")
        await monitor.send_frame(test_data)
        
        # Monitor for responses
        print("Monitoring for responses (5 seconds)...")
        await monitor.monitor(duration=5)
        
        # Print stats
        print("\nStatistics:")
        monitor.print_stats()
        
        # Close connection
        await monitor.close()
        print("\nConnection closed.")
        
    except ConnectionRefusedError:
        print(f"Error: Could not connect to {host}:{port}")
        print("Make sure a TCP server is running on that port.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


async def tcp_server_example(host='0.0.0.0', port=5000):
    """Demonstrate async TCP server with SLIP."""
    
    print("=" * 60)
    print("Async TCP Server with SLIP")
    print("=" * 60)
    print(f"Starting server on {host}:{port}...")
    
    try:
        from slipspeed.async_connections import AsyncTCPServerConnection
        from slipspeed.async_slip import encode_packet_async
        
        # Create TCP server
        server = AsyncTCPServerConnection(host, port)
        await server._open()
        print("Server started!")
        
        # Start serving in background
        serve_task = asyncio.create_task(server.serve_forever())
        
        # Wait a bit for client to connect
        await asyncio.sleep(1)
        
        # Send a welcome frame when client connects
        if server._client_connected:
            welcome = b"Welcome to async SLIP server!"
            encoded = await encode_packet_async(welcome)
            await server.write(encoded)
            print(f"Sent welcome frame: {welcome}")
        
        # Monitor for incoming frames
        print("Monitoring for incoming frames (10 seconds)...")
        for _ in range(100):  # 10 seconds with 0.1s intervals
            data = await server.read(n=4096)
            if data:
                print(f"Received data: {data.hex()}")
            await asyncio.sleep(0.1)
        
        # Stop server
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
        
        await server.close()
        print("\nServer stopped.")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Run the example."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Async TCP SLIP Example')
    parser.add_argument('--mode', choices=['client', 'server'], default='client',
                        help='Run as client or server')
    parser.add_argument('--host', default='localhost',
                        help='Host to connect to (client) or bind to (server)')
    parser.add_argument('--port', type=int, default=5000,
                        help='Port number')
    
    args = parser.parse_args()
    
    if args.mode == 'client':
        await tcp_client_example(args.host, args.port)
    else:
        await tcp_server_example(args.host, args.port)


if __name__ == "__main__":
    asyncio.run(main())
