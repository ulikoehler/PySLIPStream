"""
Async connection handlers for serial ports, TCP sockets, and files.

Provides unified async interface for reading from various I/O sources using asyncio.
"""

import asyncio
from typing import Optional
from abc import ABC, abstractmethod


class AsyncConnection(ABC):
    """Abstract base class for async connections."""
    
    @abstractmethod
    async def read(self, n: int = 4096) -> bytes:
        """
        Read available data from the connection.
        
        Args:
            n: Maximum number of bytes to read
            
        Returns:
            Bytes read
        """
        pass
    
    @abstractmethod
    async def write(self, data: bytes) -> int:
        """
        Write data to the connection.
        
        Args:
            data: Bytes to write
            
        Returns:
            Number of bytes written
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""
        pass
    
    @abstractmethod
    def is_open(self) -> bool:
        """Check if connection is open."""
        pass


class AsyncTCPConnection(AsyncConnection):
    """Handler for async TCP socket connections using asyncio streams."""
    
    def __init__(self, host: str, port: int):
        """
        Initialize async TCP connection.
        
        Args:
            host: Hostname or IP address
            port: Port number
        """
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
    
    async def _open(self) -> None:
        """Open the async TCP connection."""
        self.reader, self.writer = await asyncio.open_connection(
            self.host, self.port
        )
    
    async def read(self, n: int = 4096) -> bytes:
        """Read data from TCP socket."""
        if not self.is_open():
            raise RuntimeError("Connection is not open")
        
        try:
            data = await self.reader.read(n)
            return data
        except Exception as e:
            raise RuntimeError(f"Failed to read from TCP connection: {e}")
    
    async def write(self, data: bytes) -> int:
        """Write data to TCP socket."""
        if not self.is_open():
            raise RuntimeError("Connection is not open")
        
        try:
            self.writer.write(data)
            await self.writer.drain()
            return len(data)
        except Exception as e:
            raise RuntimeError(f"Failed to write to TCP connection: {e}")
    
    async def close(self) -> None:
        """Close the TCP connection."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
    
    def is_open(self) -> bool:
        """Check if TCP connection is open."""
        return self.reader is not None and self.writer is not None


class AsyncTCPServerConnection(AsyncConnection):
    """Handler for async TCP server connections using asyncio."""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        """
        Initialize async TCP server.
        
        Args:
            host: Host to bind to (default: '0.0.0.0' for all interfaces)
            port: Port number to bind to
        """
        self.host = host
        self.port = port
        self.server: Optional[asyncio.Server] = None
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._client_connected = False
    
    async def _open(self) -> None:
        """Start the TCP server."""
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port
        )
    
    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        """Handle incoming client connection."""
        self.reader = reader
        self.writer = writer
        self._client_connected = True
    
    async def read(self, n: int = 4096) -> bytes:
        """Read data from TCP client socket."""
        if not self.is_open() or not self._client_connected:
            return b''
        
        try:
            data = await self.reader.read(n)
            return data
        except Exception:
            self._client_connected = False
            self.reader = None
            self.writer = None
            return b''
    
    async def write(self, data: bytes) -> int:
        """Write data to TCP client socket."""
        if not self.is_open() or not self._client_connected:
            return 0
        
        try:
            self.writer.write(data)
            await self.writer.drain()
            return len(data)
        except Exception:
            self._client_connected = False
            self.reader = None
            self.writer = None
            return 0
    
    async def close(self) -> None:
        """Close the TCP server and client sockets."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
    
    def is_open(self) -> bool:
        """Check if TCP server socket is open."""
        return self.server is not None
    
    async def serve_forever(self) -> None:
        """Start serving clients (blocks until closed)."""
        if self.server:
            async with self.server:
                await self.server.serve_forever()


class AsyncFileConnection(AsyncConnection):
    """Handler for async file I/O using aiofiles."""
    
    def __init__(self, filepath: str, mode: str = 'rb'):
        """
        Initialize async file connection.
        
        Args:
            filepath: Path to the file
            mode: File mode (default: 'rb' for read binary)
        """
        self.filepath = filepath
        self.mode = mode
        self.file = None
    
    async def _open(self) -> None:
        """Open the file asynchronously."""
        try:
            import aiofiles
        except ImportError:
            raise ImportError(
                "aiofiles is required for async file operations. "
                "Install with: pip install aiofiles"
            )
        
        self.file = await aiofiles.open(self.filepath, self.mode)
    
    async def read(self, n: int = 4096) -> bytes:
        """Read data from file."""
        if not self.is_open():
            raise RuntimeError("File is not open")
        
        try:
            data = await self.file.read(n)
            return data
        except Exception as e:
            raise RuntimeError(f"Failed to read from file: {e}")
    
    async def write(self, data: bytes) -> int:
        """Write data to file."""
        if not self.is_open():
            raise RuntimeError("File is not open")
        
        try:
            written = await self.file.write(data)
            return written
        except Exception as e:
            raise RuntimeError(f"Failed to write to file: {e}")
    
    async def close(self) -> None:
        """Close the file."""
        if self.file:
            try:
                await self.file.close()
            except Exception:
                pass
            self.file = None
    
    def is_open(self) -> bool:
        """Check if file is open."""
        return self.file is not None and not self.file.closed


class AsyncSerialConnection(AsyncConnection):
    """Handler for async serial port connections using pyserial-asyncio."""
    
    def __init__(self, port: str, baudrate: int = 115200):
        """
        Initialize async serial connection.
        
        Args:
            port: Serial port name (e.g., '/dev/ttyUSB0', 'COM3')
            baudrate: Baud rate (default: 115200)
        """
        self.port = port
        self.baudrate = baudrate
        self.reader = None
        self.writer = None
    
    async def _open(self) -> None:
        """Open the async serial connection."""
        try:
            import serial_asyncio
        except ImportError:
            raise ImportError(
                "pyserial-asyncio is required for async serial connections. "
                "Install with: pip install pyserial-asyncio"
            )
        
        loop = asyncio.get_event_loop()
        self.reader, self.writer = await serial_asyncio.open_serial_connection(
            loop=loop,
            url=self.port,
            baudrate=self.baudrate
        )
    
    async def read(self, n: int = 4096) -> bytes:
        """Read data from serial port."""
        if not self.is_open():
            raise RuntimeError("Serial connection is not open")
        
        try:
            data = await self.reader.read(n)
            return data
        except Exception as e:
            raise RuntimeError(f"Failed to read from serial port: {e}")
    
    async def write(self, data: bytes) -> int:
        """Write data to serial port."""
        if not self.is_open():
            raise RuntimeError("Serial connection is not open")
        
        try:
            self.writer.write(data)
            await self.writer.drain()
            return len(data)
        except Exception as e:
            raise RuntimeError(f"Failed to write to serial port: {e}")
    
    async def close(self) -> None:
        """Close the serial connection."""
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
            self.writer = None
            self.reader = None
    
    def is_open(self) -> bool:
        """Check if serial connection is open."""
        return self.reader is not None and self.writer is not None


async def create_async_connection(connection_string: str) -> AsyncConnection:
    """
    Create an async connection from a connection string.
    
    Args:
        connection_string: 
            - For TCP client: 'tcp:192.168.1.1:5000' or 'tcp://192.168.1.1:5000'
            - For TCP server: 'tcp-listen:5000' or 'tcp-listen:0.0.0.0:5000'
            - For serial: 'serial:///dev/ttyUSB0' or 'serial:///dev/ttyUSB0:115200'
            - For file: 'file:/path/to/file' or 'file:/path/to/file:rb'
        
    Returns:
        AsyncConnection object
    """
    # Handle URL-style schemes with //
    if connection_string.startswith('tcp://'):
        tcp_part = connection_string[6:]
        parts = tcp_part.rsplit(':', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid TCP connection string: {connection_string}")
        host, port_str = parts
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port number in connection string: {port_str}")
        conn = AsyncTCPConnection(host, port)
        await conn._open()
        return conn
    
    elif connection_string.startswith('serial://'):
        serial_part = connection_string[9:]
        if ':' in serial_part:
            port, baud_str = serial_part.rsplit(':', 1)
            try:
                baudrate = int(baud_str)
            except ValueError:
                raise ValueError(f"Invalid baudrate in connection string: {baud_str}")
        else:
            port = serial_part
            baudrate = 115200
        conn = AsyncSerialConnection(port, baudrate)
        await conn._open()
        return conn
    
    elif connection_string.startswith('file://'):
        file_path = connection_string[7:]
        if ':' in file_path:
            parts = file_path.rsplit(':', 1)
            if len(parts) == 2:
                filepath, mode = parts
                conn = AsyncFileConnection(filepath, mode)
                await conn._open()
                return conn
        conn = AsyncFileConnection(file_path)
        await conn._open()
        return conn
    
    elif connection_string.startswith('tcp-listen:'):
        tcp_part = connection_string[11:]
        if ':' in tcp_part:
            parts = tcp_part.rsplit(':', 1)
            if len(parts) == 2:
                host, port_str = parts
                try:
                    port = int(port_str)
                except ValueError:
                    raise ValueError(f"Invalid port number in connection string: {port_str}")
                conn = AsyncTCPServerConnection(host, port)
                await conn._open()
                return conn
        try:
            port = int(tcp_part)
            conn = AsyncTCPServerConnection('0.0.0.0', port)
            await conn._open()
            return conn
        except ValueError:
            raise ValueError(f"Invalid TCP listen connection string: {connection_string}")
    
    elif connection_string.startswith('tcp:'):
        tcp_part = connection_string[4:]
        parts = tcp_part.rsplit(':', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid TCP connection string: {connection_string}")
        host, port_str = parts
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port number in connection string: {port_str}")
        conn = AsyncTCPConnection(host, port)
        await conn._open()
        return conn
    
    elif connection_string.startswith('file:'):
        file_path = connection_string[5:]
        if ':' in file_path:
            parts = file_path.rsplit(':', 1)
            if len(parts) == 2:
                filepath, mode = parts
                conn = AsyncFileConnection(filepath, mode)
                await conn._open()
                return conn
        conn = AsyncFileConnection(file_path)
        await conn._open()
        return conn
    
    else:
        raise ValueError(f"Unsupported connection string format: {connection_string}")
