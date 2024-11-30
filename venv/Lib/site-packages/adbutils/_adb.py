#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Created on Fri May 06 2022 10:58:29 by codeskyblue
"""

import os
import socket
import subprocess
import typing
import weakref
from typing import Iterator, Union

from deprecation import deprecated

from adbutils._utils import adb_path
from adbutils.errors import AdbError, AdbTimeout

from ._proto import *
from ._utils import list2cmdline
from ._version import __version__

_OKAY = "OKAY"
_FAIL = "FAIL"


def _check_server(host: str, port: int) -> bool:
    """ Returns if server is running """
    s = socket.socket()
    try:
        s.connect((host, port))
        return True
    except socket.error as e:
        return False
    finally:
        s.close()



class AdbConnection(object):
    def __init__(self, host: str, port: int):
        self.__host = host
        self.__port = port
        self.__conn = self._safe_connect()

        self._finalizer = weakref.finalize(self, self.conn.close)

    def _create_socket(self):
        adb_host = self.__host
        adb_port = self.__port
        s = socket.socket()
        try:
            s.connect((adb_host, adb_port))
            return s
        except:
            s.close()
            raise

    def _safe_connect(self):
        try:
            return self._create_socket()
        except ConnectionRefusedError:
            subprocess.run([adb_path(), "start-server"], timeout=20.0)  # 20s should enough for adb start
            return self._create_socket()

    @property
    def closed(self) -> bool:
        return not self._finalizer.alive

    def close(self):
        self._finalizer()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    @property
    def conn(self) -> socket.socket:
        return self.__conn
    
    def send(self, data: bytes) -> int:
        return self.conn.send(data)

    def read(self, n: int) -> bytes:
        try:
            return self._read_fully(n)
        except socket.timeout:
            raise AdbTimeout("adb read timeout")

    def _read_fully(self, n: int) -> bytes:
        t = n
        buffer = b''
        while t > 0:
            chunk = self.conn.recv(t)
            if not chunk:
                break
            buffer += chunk
            t = n - len(buffer)
        return buffer

    def send_command(self, cmd: str):
        self.conn.send("{:04x}{}".format(len(cmd), cmd).encode("utf-8"))

    def read_string(self, n: int) -> str:
        data = self.read(n).decode()
        return data

    def read_string_block(self) -> str:
        """
        Raises:
            AdbError
        """
        length = self.read_string(4)
        if not length:
            raise AdbError("connection closed")
        size = int(length, 16)
        return self.read_string(size)

    def read_until_close(self) -> str:
        content = b""
        while True:
            chunk = self.read(4096)
            if not chunk:
                break
            content += chunk
        return content.decode('utf-8', errors='ignore')

    def check_okay(self):
        data = self.read_string(4)
        if data == _FAIL:
            raise AdbError(self.read_string_block())
        elif data == _OKAY:
            return
        raise AdbError("Unknown data: %r" % data)


class BaseClient(object):
    def __init__(self, host: str = None, port: int = None, socket_timeout: float = None):
        """
        Args:
            host (str): default value from env:ANDROID_ADB_SERVER_HOST
            port (int): default value from env:ANDROID_ADB_SERVER_PORT
        """
        if not host:
            host = os.environ.get("ANDROID_ADB_SERVER_HOST", "127.0.0.1")
        if not port:
            port = int(os.environ.get("ANDROID_ADB_SERVER_PORT", 5037))
        self.__host = host
        self.__port = port
        self.__socket_timeout = socket_timeout

    @property
    def host(self) -> str:
        return self.__host
    
    @property
    def port(self) -> int:
        return self.__port
        
    def _connect(self, timeout: float = None) -> AdbConnection:
        """ connect to adb server
        
        Raises:
            AdbTimeout
        """
        timeout = timeout or self.__socket_timeout
        try:
            _conn = AdbConnection(self.__host, self.__port)
            if timeout:
                _conn.conn.settimeout(timeout)
            return _conn
        except TimeoutError:
            raise AdbTimeout("connect to adb server timeout")

    def server_version(self):
        """ 40 will match 1.0.40
        Returns:
            int
        """
        with self._connect() as c:
            c.send_command("host:version")
            c.check_okay()
            return int(c.read_string_block(), 16)

    def server_kill(self):
        """
        adb kill-server

        Send host:kill if adb-server is alive
        """
        if _check_server(self.__host, self.__port):
            with self._connect() as c:
                c.send_command("host:kill")
                c.check_okay()

    def wait_for(self, serial: str = None, transport: str = 'any', state: str = "device", timeout: float=60):
        """ Same as wait-for-TRANSPORT-STATE
        Args:
            serial (str): device serial [default None]
            transport (str): {any,usb,local} [default any]
            state (str): {device,recovery,rescue,sideload,bootloader,disconnect} [default device]
            timeout (float): max wait time [default 60]
        
        Raises:
            AdbError, AdbTimeout
        """
        with self._connect(timeout=timeout) as c:
            cmds = []
            if serial:
                cmds.extend(['host-serial', serial])
            else:
                cmds.append('host')
            cmds.append("wait-for-" + transport + "-" + state)
            c.send_command(":".join(cmds))
            c.check_okay()
            c.check_okay()

    # def reconnect(self, addr: str, timeout: float=None) -> str:
    #     """ this function is not same as adb reconnect
    #     actually the behavior is same as
    #         - adb disconnect x.x.x.x
    #         - adb connect x.x.x.x
    #     """
    #     self.disconnect(addr)
    #     return self.connect(addr, timeout=timeout)

    def connect(self, addr: str, timeout: float=None) -> str:
        """ adb connect $addr
        Args:
            addr (str): adb remote address [eg: 191.168.0.1:5555]
            timeout (float): connect timeout

        Returns:
            content adb server returns
        
        Raises:
            AdbTimeout

        Example returns:
            - "already connected to 192.168.190.101:5555"
            - "unable to connect to 192.168.190.101:5551"
            - "failed to connect to '1.2.3.4:4567': Operation timed out"
        """
        with self._connect(timeout=timeout) as c:
            c.send_command("host:connect:" + addr)
            c.check_okay()
            return c.read_string_block()

    def disconnect(self, addr: str, raise_error: bool=False) -> str:
        """ adb disconnect $addr
        Returns:
            content adb server returns

        Raises:
            when raise_error set to True
                AdbError("error: no such device '1.2.3.4:5678')

        Example returns:
            - "disconnected 192.168.190.101:5555"
        """
        try:
            with self._connect() as c:
                c.send_command("host:disconnect:" + addr)
                c.check_okay()
                return c.read_string_block()
        except AdbError:
            if raise_error:
                raise

    def track_devices(self) -> Iterator[DeviceEvent]:
        """
        Report device state when changes

        Args:
            limit_status: eg, ['device', 'offline'], empty means all status

        Returns:
            Iterator[DeviceEvent], DeviceEvent.status can be one of ['device', 'offline', 'unauthorized', 'absent']

        Raises:
            AdbError when adb-server was killed
        """
        orig_devices = []

        with self._connect() as c:
            c.send_command("host:track-devices")
            c.check_okay()
            while True:
                output = c.read_string_block()
                curr_devices = self._output2devices(output)
                for event in self._diff_devices(orig_devices, curr_devices):
                    yield event
                orig_devices = curr_devices

    def _output2devices(self, output: str):
        devices = []
        for line in output.splitlines():
            fields = line.strip().split("\t", maxsplit=1)
            if len(fields) != 2:
                continue
            serial, status = fields
            devices.append(DeviceEvent(None, serial, status))
        return devices

    def _diff_devices(self, orig: typing.List[DeviceEvent], curr: typing.List[DeviceEvent]):
        for d in set(orig).difference(curr):
            yield DeviceEvent(False, d.serial, 'absent')
        for d in set(curr).difference(orig):
            yield DeviceEvent(True, d.serial, d.status)

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__)
    def forward_list(self, serial: Union[None, str] = None):
        with self._connect() as c:
            list_cmd = "host:list-forward"
            if serial:
                list_cmd = "host-serial:{}:list-forward".format(serial)
            c.send_command(list_cmd)
            c.check_okay()
            content = c.read_string_block()
            for line in content.splitlines():
                parts = line.split()
                if len(parts) != 3:
                    continue
                if serial and parts[0] != serial:
                    continue
                yield ForwardItem(*parts)

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__)
    def forward(self, serial, local, remote, norebind=False):
        """
        Args:
            serial (str): device serial
            local, remote (str): tcp:<port> or localabstract:<name>
            norebind (bool): fail if already forwarded when set to true

        Raises:
            AdbError
        """
        with self._connect() as c:
            cmds = ["host-serial", serial, "forward"]
            if norebind:
                cmds.append("norebind")
            cmds.append(local + ";" + remote)
            c.send_command(":".join(cmds))
            c.check_okay()

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__)
    def reverse(self, serial, remote, local, norebind=False):
        """
        Args:
            serial (str): device serial
            remote, local (str): tcp:<port> or localabstract:<name>
            norebind (bool): fail if already reversed when set to true

        Raises:
            AdbError
        """
        with self._connect() as c:
            c.send_command("host:transport:" + serial)
            c.check_okay()
            cmds = ['reverse:forward', remote + ";" + local]
            c.send_command(":".join(cmds))
            c.check_okay()

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__)
    def reverse_list(self, serial: Union[None, str] = None):
        with self._connect() as c:
            c.send_command("host:transport:" + serial)
            c.check_okay()
            c.send_command("reverse:list-forward")
            c.check_okay()
            content = c.read_string_block()
            for line in content.splitlines():
                parts = line.split()
                if len(parts) != 3:
                    continue
                yield ReverseItem(*parts[1:])



