"""
Connection handlers for serial ports, TCP sockets, UDP sockets, and files.

Provides unified interface for reading from various I/O sources.
"""

import socket
import time
from typing import Optional, Union, Tuple
from abc import ABC, abstractmethod


class Connection(ABC):
    """Abstract base class for connections."""
    
    @abstractmethod
    def read(self, timeout: Optional[float] = None) -> bytes:
        """
        Read available data from the connection.
        
        Args:
            timeout: Timeout in seconds (None for blocking)
            
        Returns:
            Bytes read, or empty bytes if timeout
        """
        pass
    
    @abstractmethod
    def write(self, data: bytes) -> int:
        """
        Write data to the connection.
        
        Args:
            data: Bytes to write
            
        Returns:
            Number of bytes written
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the connection."""
        pass
    
    @abstractmethod
    def is_open(self) -> bool:
        """Check if connection is open."""
        pass


class SerialConnection(Connection):
    """Handler for serial port connections."""
    
    def __init__(self, port: str, baudrate: int = 115200, timeout: float = 0.1):
        """
        Initialize serial connection.
        
        Args:
            port: Serial port name (e.g., '/dev/ttyUSB0', 'COM3')
            baudrate: Baud rate (default: 115200)
            timeout: Read timeout in seconds
        """
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial = None
        self._open()
    
    def _open(self) -> None:
        """Open the serial connection."""
        try:
            import serial
        except ImportError:
            raise ImportError("pyserial is required for serial connections. Install with: pip install pyserial")
        
        try:
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout
            )
        except Exception as e:
            raise RuntimeError(f"Failed to open serial port {self.port}: {e}")
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from serial port."""
        if not self.is_open():
            return b''
        
        if timeout is not None:
            old_timeout = self.serial.timeout
            self.serial.timeout = timeout
            data = self.serial.read(4096)
            self.serial.timeout = old_timeout
            return data
        else:
            return self.serial.read(4096)
    
    def write(self, data: bytes) -> int:
        """Write data to serial port."""
        if not self.is_open():
            return 0
        return self.serial.write(data)
    
    def close(self) -> None:
        """Close the serial connection."""
        if self.serial:
            self.serial.close()
            self.serial = None
    
    def is_open(self) -> bool:
        """Check if serial connection is open."""
        return self.serial is not None and self.serial.is_open


class TCPConnection(Connection):
    """Handler for TCP socket connections."""
    
    def __init__(self, host: str, port: int, timeout: float = 0.1):
        """
        Initialize TCP connection.
        
        Args:
            host: Hostname or IP address
            port: Port number
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self._open()
    
    def _open(self) -> None:
        """Open the TCP connection."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
        except Exception as e:
            self.socket = None
            raise RuntimeError(f"Failed to connect to {self.host}:{self.port}: {e}")
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from TCP socket."""
        if not self.is_open():
            return b''
        
        try:
            if timeout is not None:
                self.socket.settimeout(timeout)
                data = self.socket.recv(4096)
                self.socket.settimeout(self.timeout)
                return data
            else:
                return self.socket.recv(4096)
        except socket.timeout:
            return b''
        except Exception:
            return b''
    
    def write(self, data: bytes) -> int:
        """Write data to TCP socket."""
        if not self.is_open():
            return 0
        
        try:
            return self.socket.send(data)
        except Exception:
            return 0
    
    def close(self) -> None:
        """Close the TCP connection."""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
    
    def is_open(self) -> bool:
        """Check if TCP connection is open."""
        return self.socket is not None


class FileConnection(Connection):
    """Handler for file I/O connections."""
    
    def __init__(self, filepath: str, mode: str = 'rb', timeout: float = 0.1):
        """
        Initialize file connection.
        
        Args:
            filepath: Path to the file
            mode: File mode (default: 'rb' for read binary)
            timeout: Not used for files, kept for interface compatibility
        """
        self.filepath = filepath
        self.mode = mode
        self.timeout = timeout
        self.file = None
        self._open()
    
    def _open(self) -> None:
        """Open the file."""
        try:
            self.file = open(self.filepath, self.mode)
        except Exception as e:
            raise RuntimeError(f"Failed to open file {self.filepath}: {e}")
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from file."""
        if not self.is_open():
            return b''
        
        try:
            # Read in chunks
            return self.file.read(4096)
        except Exception:
            return b''
    
    def write(self, data: bytes) -> int:
        """Write data to file."""
        if not self.is_open():
            return 0
        
        try:
            return self.file.write(data)
        except Exception:
            return 0
    
    def close(self) -> None:
        """Close the file."""
        if self.file:
            try:
                self.file.close()
            except Exception:
                pass
            self.file = None
    
    def is_open(self) -> bool:
        """Check if file is open."""
        return self.file is not None and not self.file.closed


class UDPConnection(Connection):
    """Handler for UDP socket connections (client mode)."""
    
    def __init__(self, host: str, port: int, timeout: float = 0.1, bind_port: Optional[int] = None):
        """
        Initialize UDP connection.
        
        Args:
            host: Remote hostname or IP address
            port: Remote port number
            timeout: Socket timeout in seconds
            bind_port: Local port to bind to (None for any available port)
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.bind_port = bind_port
        self.socket = None
        self.remote_addr = (host, port)
        self._open()
    
    def _open(self) -> None:
        """Open the UDP socket."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(self.timeout)
            
            # Bind to local port if specified
            if self.bind_port is not None:
                self.socket.bind(('0.0.0.0', self.bind_port))
        except Exception as e:
            self.socket = None
            raise RuntimeError(f"Failed to create UDP socket: {e}")
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from UDP socket."""
        if not self.is_open():
            return b''
        
        try:
            if timeout is not None:
                self.socket.settimeout(timeout)
                data, _ = self.socket.recvfrom(4096)
                self.socket.settimeout(self.timeout)
                return data
            else:
                data, _ = self.socket.recvfrom(4096)
                return data
        except socket.timeout:
            return b''
        except Exception:
            return b''
    
    def write(self, data: bytes) -> int:
        """Write data to UDP socket."""
        if not self.is_open():
            return 0
        
        try:
            return self.socket.sendto(data, self.remote_addr)
        except Exception:
            return 0
    
    def close(self) -> None:
        """Close the UDP socket."""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
    
    def is_open(self) -> bool:
        """Check if UDP socket is open."""
        return self.socket is not None


class UDPServerConnection(Connection):
    """Handler for UDP server connections (listen mode)."""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 5000, timeout: float = 0.1):
        """
        Initialize UDP server.
        
        Args:
            host: Host to bind to (default: '0.0.0.0' for all interfaces)
            port: Port number to bind to
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self._open()
    
    def _open(self) -> None:
        """Open the UDP server socket."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.settimeout(self.timeout)
            self.socket.bind((self.host, self.port))
        except Exception as e:
            self.socket = None
            raise RuntimeError(f"Failed to bind UDP server to {self.host}:{self.port}: {e}")
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from UDP server socket."""
        if not self.is_open():
            return b''
        
        try:
            if timeout is not None:
                self.socket.settimeout(timeout)
                data, addr = self.socket.recvfrom(4096)
                self.socket.settimeout(self.timeout)
                return data
            else:
                data, addr = self.socket.recvfrom(4096)
                return data
        except socket.timeout:
            return b''
        except Exception:
            return b''
    
    def write(self, data: bytes) -> int:
        """Write data to the last received peer (or fail if no peer)."""
        if not self.is_open():
            return 0
        # Note: UDP server doesn't have a default peer
        # This is a limitation - caller would need to track peer addresses
        # For now, return 0 to indicate no write capability
        return 0
    
    def sendto(self, data: bytes, addr: Tuple[str, int]) -> int:
        """Send data to a specific address."""
        if not self.is_open():
            return 0
        
        try:
            return self.socket.sendto(data, addr)
        except Exception:
            return 0
    
    def close(self) -> None:
        """Close the UDP server socket."""
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
    
    def is_open(self) -> bool:
        """Check if UDP server socket is open."""
        return self.socket is not None


class TCPServerConnection(Connection):
    """Handler for TCP server connections (listen mode)."""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 5000, timeout: float = 0.1):
        """
        Initialize TCP server.
        
        Args:
            host: Host to bind to (default: '0.0.0.0' for all interfaces)
            port: Port number to bind to
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.server_socket = None
        self.client_socket = None
        self._open()
    
    def _open(self) -> None:
        """Open the TCP server socket."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.settimeout(self.timeout)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
        except Exception as e:
            self.server_socket = None
            raise RuntimeError(f"Failed to bind TCP server to {self.host}:{self.port}: {e}")
    
    def _accept_client(self) -> bool:
        """Accept a client connection if none exists."""
        if self.client_socket is None:
            try:
                self.client_socket, addr = self.server_socket.accept()
                self.client_socket.settimeout(self.timeout)
                return True
            except socket.timeout:
                return False
            except Exception:
                return False
        return True
    
    def read(self, timeout: Optional[float] = None) -> bytes:
        """Read data from TCP client socket."""
        if not self.is_open():
            return b''
        
        # Try to accept a client if none exists
        if not self._accept_client():
            return b''
        
        try:
            if timeout is not None:
                self.client_socket.settimeout(timeout)
                data = self.client_socket.recv(4096)
                self.client_socket.settimeout(self.timeout)
                return data
            else:
                return self.client_socket.recv(4096)
        except socket.timeout:
            return b''
        except Exception:
            # Client disconnected, close it
            self.client_socket = None
            return b''
    
    def write(self, data: bytes) -> int:
        """Write data to TCP client socket."""
        if not self.is_open() or self.client_socket is None:
            return 0
        
        try:
            return self.client_socket.send(data)
        except Exception:
            self.client_socket = None
            return 0
    
    def close(self) -> None:
        """Close the TCP server and client sockets."""
        if self.client_socket:
            try:
                self.client_socket.close()
            except Exception:
                pass
            self.client_socket = None
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
            self.server_socket = None
    
    def is_open(self) -> bool:
        """Check if TCP server socket is open."""
        return self.server_socket is not None


def create_connection(connection_string: str, timeout: float = 0.1) -> Connection:
    """
    Create a connection from a connection string.
    
    Args:
        connection_string: 
            - For serial: '/dev/ttyUSB0' or 'COM3' or '/dev/ttyUSB0:115200'
            - For TCP client: 'tcp:192.168.1.1:5000' or 'localhost:5000'
            - For TCP server: 'tcp-listen:5000' or 'tcp-listen:0.0.0.0:5000'
            - For UDP client: 'udp:192.168.1.1:5000' or 'udp:192.168.1.1:5000:8000' (last is local bind port)
            - For UDP server: 'udp-listen:5000' or 'udp-listen:0.0.0.0:5000'
            - For file: 'file:/path/to/file' or 'file:/path/to/file:rb' (mode optional)
        timeout: Read timeout in seconds
        
    Returns:
        Connection object (SerialConnection, TCPConnection, TCPServerConnection,
                          UDPConnection, UDPServerConnection, or FileConnection)
    """
    if connection_string.startswith('tcp-listen:'):
        # TCP server connection
        tcp_part = connection_string[11:]
        if ':' in tcp_part:
            parts = tcp_part.rsplit(':', 1)
            if len(parts) == 2:
                host, port_str = parts
                try:
                    port = int(port_str)
                except ValueError:
                    raise ValueError(f"Invalid port number in connection string: {port_str}")
                return TCPServerConnection(host, port, timeout=timeout)
        try:
            port = int(tcp_part)
            return TCPServerConnection('0.0.0.0', port, timeout=timeout)
        except ValueError:
            raise ValueError(f"Invalid TCP listen connection string: {connection_string}")
    
    elif connection_string.startswith('tcp:'):
        # TCP client connection
        tcp_part = connection_string[4:]
        parts = tcp_part.rsplit(':', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid TCP connection string: {connection_string}")
        host, port_str = parts
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port number in connection string: {port_str}")
        return TCPConnection(host, port, timeout=timeout)
    
    elif connection_string.startswith('udp-listen:'):
        # UDP server connection
        udp_part = connection_string[11:]
        if ':' in udp_part:
            parts = udp_part.rsplit(':', 1)
            if len(parts) == 2:
                host, port_str = parts
                try:
                    port = int(port_str)
                except ValueError:
                    raise ValueError(f"Invalid port number in connection string: {port_str}")
                return UDPServerConnection(host, port, timeout=timeout)
        try:
            port = int(udp_part)
            return UDPServerConnection('0.0.0.0', port, timeout=timeout)
        except ValueError:
            raise ValueError(f"Invalid UDP listen connection string: {connection_string}")
    
    elif connection_string.startswith('udp:'):
        # UDP client connection
        udp_part = connection_string[4:]
        parts = udp_part.split(':')
        if len(parts) == 2:
            host, port_str = parts
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"Invalid port number in connection string: {port_str}")
            return UDPConnection(host, port, timeout=timeout)
        elif len(parts) == 3:
            host, port_str, bind_port_str = parts
            try:
                port = int(port_str)
                bind_port = int(bind_port_str)
            except ValueError:
                raise ValueError(f"Invalid port number in connection string")
            return UDPConnection(host, port, timeout=timeout, bind_port=bind_port)
        else:
            raise ValueError(f"Invalid UDP connection string: {connection_string}")
    
    elif connection_string.startswith('file:'):
        # File connection
        file_path = connection_string[5:]
        if ':' in file_path:
            parts = file_path.rsplit(':', 1)
            if len(parts) == 2:
                filepath, mode = parts
                return FileConnection(filepath, mode=mode, timeout=timeout)
        return FileConnection(file_path, timeout=timeout)
    
    else:
        # Serial connection
        if ':' in connection_string:
            port, baud_str = connection_string.rsplit(':', 1)
            try:
                baudrate = int(baud_str)
            except ValueError:
                raise ValueError(f"Invalid baudrate in connection string: {baud_str}")
        else:
            port = connection_string
            baudrate = 115200
        
        return SerialConnection(port, baudrate=baudrate, timeout=timeout)
