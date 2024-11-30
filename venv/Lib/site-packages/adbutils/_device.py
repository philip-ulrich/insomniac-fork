#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Created on Fri May 06 2022 10:33:39 by codeskyblue
"""

import datetime
import io
import json
import os
import pathlib
import re
import socket
import stat
import struct
import subprocess
import tempfile
import textwrap
import threading
import time
import typing
import warnings
from contextlib import contextmanager
from dataclasses import asdict
from typing import Optional, Union

import apkutils2
import requests
from deprecation import deprecated
from PIL import Image, UnidentifiedImageError
from retry import retry

from ._adb import AdbConnection, BaseClient
from ._proto import *
from ._utils import (APKReader, ReadProgress, adb_path, get_free_port,
                     humanize, list2cmdline)
from ._version import __version__
from .errors import AdbError, AdbInstallError

_DISPLAY_RE = re.compile(
    r'.*DisplayViewport{.*?valid=true, .*?orientation=(?P<orientation>\d+), .*?deviceWidth=(?P<width>\d+), deviceHeight=(?P<height>\d+).*'
)


class BaseDevice:
    """ Basic operation for a device """
    def __init__(self, client: BaseClient, serial: str = None, transport_id: int = None):
        self._client = client
        self._serial = serial
        self._transport_id: int = transport_id
        self._properties = {}  # store properties data

        if not serial and not transport_id:
            raise AdbError("serial, transport_id must set atleast one")

    @property
    def serial(self) -> str:
        return self._serial

    def open_transport(self, command: str = None, timeout: float = None) -> AdbConnection:
        # connect has it own timeout
        c = self._client._connect()
        if timeout:
            c.conn.settimeout(timeout)

        if command:
            if self._transport_id:
                c.send_command(f"host-transport-id:{self._transport_id}:{command}")
            elif self._serial:
                c.send_command(f"host-serial:{self._serial}:{command}")
            else:
                raise RuntimeError
            c.check_okay()
        else:
            if self._transport_id:
                c.send_command(f"host:transport-id:{self._transport_id}")
            elif self._serial:
                # host:tport:serial:xxx is also fine, but receive 12 bytes
                # recv: 4f 4b 41 59 14 00 00 00 00 00 00 00              OKAY........
                # so here use host:transport
                c.send_command(f"host:transport:{self._serial}")
            else:
                raise RuntimeError
            c.check_okay()
        return c

    def _get_with_command(self, cmd: str) -> str:
        c = self.open_transport(cmd)
        return c.read_string_block()

    def get_state(self) -> str:
        """ return device state {offline,bootloader,device} """
        return self._get_with_command("get-state")

    def get_serialno(self) -> str:
        """ return the real device id, not the connect serial """
        return self._get_with_command("get-serialno")

    def get_devpath(self) -> str:
        """ example return: usb:12345678Y """
        return self._get_with_command("get-devpath")

    def get_features(self) -> str:
        """
        Return example:
            'abb_exec,fixed_push_symlink_timestamp,abb,stat_v2,apex,shell_v2,fixed_push_mkdir,cmd'
        """
        return self._get_with_command("features")

    @property
    def info(self) -> dict:
        return {
            "serialno": self.get_serialno(),
            "devpath": self.get_devpath(),
            "state": self.get_state(),
        }

    def __repr__(self):
        return "AdbDevice(serial={})".format(self.serial)

    @property
    def sync(self) -> 'Sync':
        return Sync(self._client, self.serial)

    @property
    def prop(self) -> "Property":
        return Property(self)

    def adb_output(self, *args, **kwargs):
        """Run adb command use subprocess and get its content

        Returns:
            string of output

        Raises:
            EnvironmentError
        """
        cmds = [adb_path(), '-s', self._serial
                ] if self._serial else [adb_path()]
        cmds.extend(args)
        cmdline = list2cmdline(map(str, cmds))
        try:
            return subprocess.check_output(cmdline,
                                           stdin=subprocess.DEVNULL,
                                           stderr=subprocess.STDOUT,
                                           shell=True).decode('utf-8')
        except subprocess.CalledProcessError as e:
            if kwargs.get('raise_error', True):
                raise EnvironmentError(
                    "subprocess", cmdline,
                    e.output.decode('utf-8', errors='ignore'))

    def shell(self,
              cmdargs: Union[str, list, tuple],
              stream: bool = False,
              timeout: Optional[float] = None,
              rstrip=True) -> typing.Union[AdbConnection, str]:
        """Run shell inside device and get it's content

        Args:
            rstrip (bool): strip the last empty line (Default: True)
            stream (bool): return stream instead of string output (Default: False)
            timeout (float): set shell timeout

        Returns:
            string of output when stream is False
            AdbConnection when stream is True

        Raises:
            AdbTimeout

        Examples:
            shell("ls -l")
            shell(["ls", "-l"])
            shell("ls | grep data")
        """
        if isinstance(cmdargs, (list, tuple)):
            cmdargs = list2cmdline(cmdargs)
        if stream:
            timeout = None
        c = self.open_transport(timeout=timeout)
        c.send_command("shell:" + cmdargs)
        c.check_okay()
        if stream:
            return c
        output = c.read_until_close()
        return output.rstrip() if rstrip else output

    def shell2(self,
              cmdargs: Union[str, list, tuple],
              timeout: Optional[float] = None,
              rstrip=True) -> ShellReturn:
        """
        Run shell command with detail output

        Returns:
            ShellOutput
        
        Raises:
            AdbTimeout
        """
        if isinstance(cmdargs, (list, tuple)):
            cmdargs = list2cmdline(cmdargs)
        assert isinstance(cmdargs, str)
        MAGIC = "X4EXIT:"
        newcmd = cmdargs + f"; echo {MAGIC}$?"
        output = self.shell(newcmd, timeout=timeout, rstrip=rstrip)
        rindex = output.rfind(MAGIC)
        if rindex == -1:  # normally will not possible
            raise AdbError("shell output invalid", output)
        returncoode = int(output[rindex + len(MAGIC):])
        return ShellReturn(command=cmdargs,
                           returncode=returncoode,
                           output=output[:rindex])

    def forward(self, local: str, remote: str, norebind: bool = False):
        args = ["forward"]
        if norebind:
            args.append("norebind")
        args.append(local+";" + remote)
        self.open_transport(":".join(args))

    def forward_port(self, remote: Union[int, str]) -> int:
        """ forward remote port to local random port """
        if isinstance(remote, int):
            remote = "tcp:" + str(remote)
        for f in self.forward_list():
            if f.serial == self._serial and f.remote == remote and f.local.startswith("tcp:"):
                return int(f.local[len("tcp:"):])
        local_port = get_free_port()
        self.forward("tcp:" + str(local_port), remote)
        return local_port

    def forward_list(self):
        c = self.open_transport("list-forward")
        content = c.read_string_block()
        for line in content.splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            yield ForwardItem(*parts)
        return self._client.forward_list(self._serial)

    def reverse(self, remote: str, local: str, norebind: bool = False):
        """
        Args:
            serial (str): device serial
            remote, local (str):
                - tcp:<port>
                - localabstract:<unix domain socket name>
                - localreserved:<unix domain socket name>
                - localfilesystem:<unix domain socket name>
            norebind (bool): fail if already reversed when set to true

        Raises:
            AdbError
        """
        args = ["forward"]
        if norebind:
            args.append("norebind")
        args.append(local+";" + remote)
        self.open_transport(":".join(args))

    def reverse_list(self):
        c = self.open_transport()
        c.send_command("reverse:list-forward")
        c.check_okay()
        content = c.read_string_block()
        for line in content.splitlines():
            parts = line.split()
            if len(parts) != 3:
                continue
            yield ReverseItem(*parts[1:])

    def push(self, local: str, remote: str):
        self.adb_output("push", local, remote)

    def create_connection(self, network: Network, address: Union[int, str]) -> socket.socket:
        """
        Used to connect a socket (unix of tcp) on the device

        Returns:
            socket object

        Raises:
            AssertionError, ValueError
        """
        c = self.open_transport()
        if network == Network.TCP:
            assert isinstance(address, int)
            c.send_command("tcp:" + str(address))
            c.check_okay()
        elif network in [Network.UNIX, Network.LOCAL_ABSTRACT]:
            assert isinstance(address, str)
            c.send_command("localabstract:" + address)
            c.check_okay()
        elif network in [Network.LOCAL_FILESYSTEM, Network.LOCAL, Network.DEV, Network.LOCAL_RESERVED]:
            c.send_command(network + ":" + address)
            c.check_okay()
        else:
            raise ValueError("Unsupported network type", network)
        c._finalizer.detach()
        return c.conn

    def root(self):
        """ restart adbd as root
        
        Return example:
            cannot run as root in production builds
        """
        # Ref: https://github.com/Swind/pure-python-adb/blob/master/ppadb/command/transport/__init__.py#L179
        c = self.open_transport()
        c.send_command("root:")
        c.check_okay()
        return c.read_string_block()

    def tcpip(self, port: int):
        """ restart adbd listening on TCP on PORT

        Return example:
            restarting in TCP mode port: 5555
        """
        c = self.open_transport()
        c.send_command("tcpip:" + str(port))
        c.check_okay()
        return c.read_until_close()


class Property():
    def __init__(self, d: BaseDevice):
        self._d = d

    def __str__(self):
        return f"product:{self.name} model:{self.model} device:{self.device}"

    def get(self, name: str, cache=True) -> str:
        if cache and name in self._d._properties:
            return self._d._properties[name]
        value = self._d._properties[name] = self._d.shell(['getprop', name]).strip()
        return value

    @property
    def name(self):
        return self.get("ro.product.name", cache=True)

    @property
    def model(self):
        return self.get("ro.product.model", cache=True)

    @property
    def device(self):
        return self.get("ro.product.device", cache=True)


_OKAY = "OKAY"
_FAIL = "FAIL"
_DENT = "DENT"  # Directory Entity
_DONE = "DONE"
_DATA = "DATA"


class Sync():
    def __init__(self, adbclient: BaseClient, serial: str):
        self._adbclient = adbclient
        self._serial = serial

    @contextmanager
    def _prepare_sync(self, path: str, cmd: str):
        c = self._adbclient._connect()
        try:
            c.send_command(":".join(["host", "transport", self._serial]))
            c.check_okay()
            c.send_command("sync:")
            c.check_okay()
            # {COMMAND}{LittleEndianPathLength}{Path}
            c.conn.send(
                cmd.encode("utf-8") + struct.pack("<I", len(path)) +
                path.encode("utf-8"))
            yield c
        finally:
            c.close()

    def exists(self, path: str) -> bool:
        finfo = self.stat(path)
        return finfo.mtime is not None

    def stat(self, path: str) -> FileInfo:
        with self._prepare_sync(path, "STAT") as c:
            assert "STAT" == c.read_string(4)
            mode, size, mtime = struct.unpack("<III", c.conn.recv(12))
            # when mtime is 0, windows will error
            mdtime = datetime.datetime.fromtimestamp(mtime) if mtime else None
            return FileInfo(mode, size, mdtime, path)

    def iter_directory(self, path: str):
        with self._prepare_sync(path, "LIST") as c:
            while 1:
                response = c.read_string(4)
                if response == _DONE:
                    break
                mode, size, mtime, namelen = struct.unpack(
                    "<IIII", c.conn.recv(16))
                name = c.read_string(namelen)
                try:
                    mtime = datetime.datetime.fromtimestamp(mtime)
                except OSError:  # bug in Python 3.6
                    mtime = datetime.datetime.now()
                yield FileInfo(mode, size, mtime, name)

    def list(self, path: str) -> typing.List[str]:
        return list(self.iter_directory(path))

    def push(self,
             src: typing.Union[pathlib.Path, str, bytes, bytearray, typing.BinaryIO],
             dst: typing.Union[pathlib.Path, str],
             mode: int = 0o755,
             check: bool = False) -> int:
        # IFREG: File Regular
        # IFDIR: File Directory
        if isinstance(src, pathlib.Path):
            src = src.open("rb")
        elif isinstance(src, str):
            src = pathlib.Path(src).open("rb")
        elif isinstance(src, (bytes, bytearray)):
            src = io.BytesIO(src)
        else:
            if not hasattr(src, "read"):
                raise TypeError("Invalid src type: %s" % type(src))

        if isinstance(dst, pathlib.Path):
            dst = dst.as_posix()
        path = dst + "," + str(stat.S_IFREG | mode)
        total_size = 0
        with self._prepare_sync(path, "SEND") as c:
            r = src if hasattr(src, "read") else open(src, "rb")
            try:
                while True:
                    chunk = r.read(4096)
                    if not chunk:
                        mtime = int(datetime.datetime.now().timestamp())
                        c.conn.send(b"DONE" + struct.pack("<I", mtime))
                        break
                    c.conn.send(b"DATA" + struct.pack("<I", len(chunk)))
                    c.conn.send(chunk)
                    total_size += len(chunk)
                assert c.read_string(4) == _OKAY
            finally:
                if hasattr(r, "close"):
                    r.close()
        if check:
            file_size = self.stat(dst).size
            if total_size != file_size:
                raise AdbError("Push not complete, expect pushed %d, actually pushed %d" % (total_size, file_size))
        return total_size

    def iter_content(self, path: str) -> typing.Iterator[bytes]:
        with self._prepare_sync(path, "RECV") as c:
            while True:
                cmd = c.read_string(4)
                if cmd == _FAIL:
                    str_size = struct.unpack("<I", c.read(4))[0]
                    error_message = c.read_string(str_size)
                    raise AdbError(error_message, path)
                elif cmd == _DONE:
                    break
                elif cmd == _DATA:
                    chunk_size = struct.unpack("<I", c.read(4))[0]
                    chunk = c.read(chunk_size)
                    if len(chunk) != chunk_size:
                        raise RuntimeError("read chunk missing")
                    yield chunk
                else:
                    raise AdbError("Invalid sync cmd", cmd)

    def read_bytes(self, path: str) -> bytes:
        return b''.join(self.iter_content(path))

    def read_text(self, path: str, encoding: str = 'utf-8') -> str:
        """ read content of a file """
        return self.read_bytes(path).decode(encoding=encoding)

    def pull(self, src: str, dst: typing.Union[str, pathlib.Path]) -> int:
        """
        Pull file from device:src to local:dst

        Returns:
            file size
        """
        if isinstance(dst, str):
            dst = pathlib.Path(dst)
        with dst.open("wb") as f:
            size = 0
            for chunk in self.iter_content(src):
                f.write(chunk)
                size += len(chunk)
            return size


class AdbDevice(BaseDevice):
    """ provide custom functions for some complex operations """

    def screenshot(self) -> Image.Image:
        """ not thread safe """
        try:
            inner_tmp_path = "/sdcard/tmp001.png"
            self.shell(['rm', inner_tmp_path])
            self.shell(["screencap", "-p", inner_tmp_path])

            with tempfile.TemporaryDirectory() as tmpdir:
                target_path = os.path.join(tmpdir, "tmp001.png")
                self.sync.pull(inner_tmp_path, target_path)
                im = Image.open(target_path)
                im.load()
                self._width, self._height = im.size
                return im.convert("RGB")
        except UnidentifiedImageError:
            w, h = self.window_size()
            return Image.new("RGB", (w, h), (220, 120, 100))

    def switch_screen(self, status: bool):
        """
        turn screen on/off

        Args:
            status (bool)
        """
        _key_dict = {
            True: '224',
            False: '223',
        }
        return self.keyevent(_key_dict[status])

    def switch_airplane(self, status: bool):
        """
        turn airplane-mode on/off

        Args:
            status (bool)
        """
        base_setting_cmd = ["settings", "put", "global", "airplane_mode_on"]
        base_am_cmd = [
            "am", "broadcast", "-a", "android.intent.action.AIRPLANE_MODE",
            "--ez", "state"
        ]
        if status:
            base_setting_cmd += ['1']
            base_am_cmd += ['true']
        else:
            base_setting_cmd += ['0']
            base_am_cmd += ['false']

        # TODO better idea about return value?
        self.shell(base_setting_cmd)
        return self.shell(base_am_cmd)

    def switch_wifi(self, status: bool) -> str:
        """
        turn WiFi on/off

        Args:
            status (bool)
        """
        arglast = 'enable' if status else 'disable'
        cmdargs = ['svc', 'wifi', arglast]
        return self.shell(cmdargs)

    def keyevent(self, key_code: typing.Union[int, str]) -> str:
        """ adb _run input keyevent KEY_CODE """
        return self.shell(['input', 'keyevent', str(key_code)])

    def click(self, x, y):
        """
        simulate android tap

        Args:
            x, y: int
        """
        x, y = map(str, [x, y])
        return self.shell(['input', 'tap', x, y])

    def swipe(self, sx, sy, ex, ey, duration: float = 1.0):
        """
        swipe from start point to end point

        Args:
            sx, sy: start point(x, y)
            ex, ey: end point(x, y)
        """
        x1, y1, x2, y2 = map(str, [sx, sy, ex, ey])
        return self.shell(
            ['input', 'swipe', x1, y1, x2, y2,
             str(int(duration * 1000))])

    def send_keys(self, text: str):
        """ 
        Type a given text 

        Args:
            text: text to be type
        """
        escaped_text = self._escape_special_characters(text)
        return self.shell(['input', 'text', escaped_text])

    @staticmethod
    def _escape_special_characters(text):
        """
        A helper that escape special characters

        Args:
            text: str
        """
        escaped = text.translate(
            str.maketrans({
                "-": r"\-",
                "+": r"\+",
                "[": r"\[",
                "]": r"\]",
                "(": r"\(",
                ")": r"\)",
                "{": r"\{",
                "}": r"\}",
                "\\": r"\\\\",
                "^": r"\^",
                "$": r"\$",
                "*": r"\*",
                ".": r"\.",
                ",": r"\,",
                ":": r"\:",
                "~": r"\~",
                ";": r"\;",
                ">": r"\>",
                "<": r"\<",
                "%": r"\%",
                "#": r"\#",
                "\'": r"\\'",
                "\"": r'\\"',
                "`": r"\`",
                "!": r"\!",
                "?": r"\?",
                "|": r"\|",
                "=": r"\=",
                "@": r"\@",
                "/": r"\/",
                "_": r"\_",
                " ": r"%s",  # special
                "&": r"\&"
            }))
        return escaped

    def wlan_ip(self) -> str:
        """
        get device wlan ip

        Raises:
            AdbError
        """
        result = self.shell(['ifconfig', 'wlan0'])
        m = re.search(r'inet\s*addr:(.*?)\s', result, re.DOTALL)
        if m:
            return m.group(1)

        # Huawei P30, has no ifconfig
        result = self.shell(['ip', 'addr', 'show', 'dev', 'wlan0'])
        m = re.search(r'inet (\d+.*?)/\d+', result)
        if m:
            return m.group(1)

        # On VirtualDevice, might use eth0
        result = self.shell(['ifconfig', 'eth0'])
        m = re.search(r'inet\s*addr:(.*?)\s', result, re.DOTALL)
        if m:
            return m.group(1)

        raise AdbError("fail to parse wlan ip")


    @retry(BrokenPipeError, delay=5.0, jitter=[3, 5], tries=3)
    def install(self,
                path_or_url: str,
                nolaunch: bool = False,
                uninstall: bool = False,
                silent: bool = False,
                callback: typing.Callable[[str], None] = None):
        """
        Install APK to device

        Args:
            path_or_url: local path or http url
            nolaunch: do not launch app after install
            uninstall: uninstall app before install
            silent: disable log message print
            callback: only two event now: <"BEFORE_INSTALL" | "FINALLY">
        
        Raises:
            AdbInstallError, BrokenPipeError
        """
        if re.match(r"^https?://", path_or_url):
            resp = requests.get(path_or_url, stream=True)
            resp.raise_for_status()
            length = int(resp.headers.get("Content-Length", 0))
            r = ReadProgress(resp.raw, length)
            print("tmpfile path:", r.filepath())
        else:
            length = os.stat(path_or_url).st_size
            fd = open(path_or_url, "rb")
            r = ReadProgress(fd, length, source_path=path_or_url)

        def _dprint(*args):
            if not silent:
                print(*args)

        dst = "/data/local/tmp/tmp-%d.apk" % (int(time.time() * 1000))
        _dprint("push to %s" % dst)

        start = time.time()
        self.sync.push(r, dst)

        # parse apk package-name
        apk = apkutils2.APK(r.filepath())
        package_name = apk.manifest.package_name
        main_activity = apk.manifest.main_activity
        if main_activity and main_activity.find(".") == -1:
            main_activity = "." + main_activity

        version_name = apk.manifest.version_name
        _dprint("packageName:", package_name)
        _dprint("mainActivity:", main_activity)
        _dprint("apkVersion: {}".format(version_name))
        _dprint("Success pushed, time used %d seconds" % (time.time() - start))

        new_dst = "/data/local/tmp/{}-{}.apk".format(package_name,
                                                     version_name)
        self.shell(["mv", dst, new_dst])

        dst = new_dst
        info = self.sync.stat(dst)
        print("verify pushed apk, md5: %s, size: %s" %
              (r._hash, humanize(info.size)))
        assert info.size == r.copied

        if uninstall:
            _dprint("Uninstall app first")
            self.uninstall(package_name)

        _dprint("install to android system ...")
        try:
            start = time.time()
            if callback:
                callback("BEFORE_INSTALL")

            self.install_remote(dst, clean=True)
            _dprint("Success installed, time used %d seconds" %
                    (time.time() - start))
            if not nolaunch:
                _dprint("Launch app: %s/%s" % (package_name, main_activity))
                self.app_start(package_name, main_activity)

        except AdbInstallError as e:
            if e.reason in [
                    "INSTALL_FAILED_PERMISSION_MODEL_DOWNGRADE",
                    "INSTALL_FAILED_UPDATE_INCOMPATIBLE",
                    "INSTALL_FAILED_VERSION_DOWNGRADE"
            ]:
                _dprint("uninstall %s because %s" % (package_name, e.reason))
                self.uninstall(package_name)
                self.install_remote(dst, clean=True)
                _dprint("Success installed, time used %d seconds" %
                        (time.time() - start))
                if not nolaunch:
                    _dprint("Launch app: %s/%s" %
                            (package_name, main_activity))
                    self.app_start(package_name, main_activity)
                    # self.shell([
                    #     'am', 'start', '-n', package_name + "/" + main_activity
                    # ])
            elif e.reason == "INSTALL_FAILED_CANCELLED_BY_USER":
                _dprint("Catch error %s, reinstall" % e.reason)
                self.install_remote(dst, clean=True)
                _dprint("Success installed, time used %d seconds" %
                        (time.time() - start))
            else:
                # print to console
                print(
                    "Failure " + e.reason + "\n" +
                    "Remote apk is not removed. Manually install command:\n\t"
                    + "adb shell pm install -r -t " + dst)
                raise
        finally:
            if callback:
                callback("FINALLY")

    def install_remote(self,
                       remote_path: str,
                       clean: bool = False,
                       flags: list = ["-r", "-t"]):
        """
        Args:
            remote_path: remote package path
            clean(bool): remove when installed, default(False)
            flags (list): default ["-r", "-t"]

        Raises:
            AdbInstallError
        """
        args = ["pm", "install"] + flags + [remote_path]
        output = self.shell(args)
        if "Success" not in output:
            raise AdbInstallError(output)
        if clean:
            self.shell(["rm", remote_path])

    def uninstall(self, pkg_name: str):
        """
        Uninstall app by package name

        Args:
            pkg_name (str): package name
        """
        return self.shell(["pm", "uninstall", pkg_name])

    def getprop(self, prop: str) -> str:
        return self.shell(['getprop', prop]).strip()

    def list_packages(self) -> typing.List[str]:
        """
        Returns:
            list of package names
        """
        result = []
        output = self.shell(["pm", "list", "packages"])
        for m in re.finditer(r'^package:([^\s]+)\r?$', output, re.M):
            result.append(m.group(1))
        return list(sorted(result))

    def package_info(self, package_name: str) -> typing.Union[dict, None]:
        """
        version_code might be empty

        Returns:
            None or dict(version_name, version_code, signature)
        """
        output = self.shell(['dumpsys', 'package', package_name])
        m = re.compile(r'versionName=(?P<name>[\w.]+)').search(output)
        version_name = m.group('name') if m else ""
        m = re.compile(r'versionCode=(?P<code>\d+)').search(output)
        version_code = m.group('code') if m else ""
        if version_code == "0":
            version_code = ""
        m = re.search(r'PackageSignatures\{.*?\[(.*)\]\}', output)
        signature = m.group(1) if m else None
        if not version_name and signature is None:
            return None
        m = re.compile(r"pkgFlags=\[\s*(.*)\s*\]").search(output)
        pkgflags = m.group(1) if m else ""
        pkgflags = pkgflags.split()

        time_regex = r"[-\d]+\s+[:\d]+"
        m = re.compile(f"firstInstallTime=({time_regex})").search(output)
        first_install_time = datetime.datetime.strptime(
            m.group(1), "%Y-%m-%d %H:%M:%S") if m else None

        m = re.compile(f"lastUpdateTime=({time_regex})").search(output)
        last_update_time = datetime.datetime.strptime(
            m.group(1).strip(), "%Y-%m-%d %H:%M:%S") if m else None

        return dict(package_name=package_name,
                    version_name=version_name,
                    version_code=version_code,
                    flags=pkgflags,
                    first_install_time=first_install_time,
                    last_update_time=last_update_time,
                    signature=signature)

    def rotation(self) -> int:
        """
        Returns:
            int [0, 1, 2, 3]
        """
        for line in self.shell('dumpsys display').splitlines():
            m = _DISPLAY_RE.search(line, 0)
            if not m:
                continue
            o = int(m.group('orientation'))
            return int(o)

        output = self.shell(
            'LD_LIBRARY_PATH=/data/local/tmp /data/local/tmp/minicap -i')
        try:
            if output.startswith('INFO:'):
                output = output[output.index('{'):]
            data = json.loads(output)
            return data['rotation'] / 90
        except ValueError:
            pass

        raise AdbError("rotation get failed")

    def _raw_window_size(self) -> WindowSize:
        output = self.shell("wm size")
        o = re.search(r"Override size: (\d+)x(\d+)", output)
        m = re.search(r"Physical size: (\d+)x(\d+)", output)
        if o:
            w, h = o.group(1), o.group(2)
            return WindowSize(int(w), int(h))
        elif m:
            w, h = m.group(1), m.group(2)
            return WindowSize(int(w), int(h))

        for line in self.shell('dumpsys display').splitlines():
            m = _DISPLAY_RE.search(line, 0)
            if not m:
                continue
            w = int(m.group('width'))
            h = int(m.group('height'))
            return WindowSize(w, h)
        raise AdbError("get window size failed")

    def window_size(self) -> WindowSize:
        """
        Return screen (width, height)

        Virtual keyborad may get small d.info['displayHeight']
        """
        w, h = self._raw_window_size()
        s, l = min(w, h), max(w, h)
        horizontal = self.rotation() % 2 == 1
        return WindowSize(l, s) if horizontal else WindowSize(s, l)

    def app_start(self, package_name: str, activity: str = None):
        """ start app with "am start" or "monkey"
        """
        if activity:
            self.shell(['am', 'start', '-n', package_name + "/" + activity])
        else:
            self.shell([
                "monkey", "-p", package_name, "-c",
                "android.intent.category.LAUNCHER", "1"
            ])

    def app_stop(self, package_name: str):
        """ stop app with "am force-stop"
        """
        self.shell(['am', 'force-stop', package_name])

    def app_clear(self, package_name: str):
        self.shell(["pm", "clear", package_name])

    def is_screen_on(self):
        output = self.shell(["dumpsys", "power"])
        return 'mHoldingDisplaySuspendBlocker=true' in output

    def open_browser(self, url: str):
        if not re.match("^https?://", url):
            url = "https://" + url
        self.shell(
            ['am', 'start', '-a', 'android.intent.action.VIEW', '-d', url])

    def dump_hierarchy(self) -> str:
        """
        uiautomator dump

        Returns:
            content of xml
        
        Raises:
            AdbError
        """
        output = self.shell(
            'uiautomator dump /data/local/tmp/uidump.xml && echo success')
        if "success" not in output:
            raise AdbError("uiautomator dump failed", output)

        buf = b''
        for chunk in self.sync.iter_content("/data/local/tmp/uidump.xml"):
            buf += chunk
        return buf.decode("utf-8")

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__,
                details="Use app_current instead")
    def current_app(self) -> dict:
        """
        Returns:
            dict(package, activity, pid?)

        Raises:
            AdbError
        """
        info = self.app_current()
        return asdict(info)

    @retry(AdbError, delay=.5, tries=3, jitter=.1)
    def app_current(self) -> RunningAppInfo:
        """
        Returns:
            RunningAppInfo(package, activity, pid?)  pid can be 0

        Raises:
            AdbError
        """
        # Related issue: https://github.com/openatx/uiautomator2/issues/200
        # $ adb shell dumpsys window windows
        # Example output:
        #   mCurrentFocus=Window{41b37570 u0 com.incall.apps.launcher/com.incall.apps.launcher.Launcher}
        #   mFocusedApp=AppWindowToken{422df168 token=Token{422def98 ActivityRecord{422dee38 u0 com.example/.UI.play.PlayActivity t14}}}
        # Regexp
        #   r'mFocusedApp=.*ActivityRecord{\w+ \w+ (?P<package>.*)/(?P<activity>.*) .*'
        #   r'mCurrentFocus=Window{\w+ \w+ (?P<package>.*)/(?P<activity>.*)\}')
        _focusedRE = re.compile(
            r'mCurrentFocus=Window{.*\s+(?P<package>[^\s]+)/(?P<activity>[^\s]+)\}'
        )
        m = _focusedRE.search(self.shell(['dumpsys', 'window', 'windows']))
        if m:
            return RunningAppInfo(package=m.group('package'),
                               activity=m.group('activity'))

        # search mResumedActivity
        # https://stackoverflow.com/questions/13193592/adb-android-getting-the-name-of-the-current-activity
        package = None
        output = self.shell(['dumpsys', 'activity', 'activities'])
        _recordRE = re.compile(r'mResumedActivity: ActivityRecord\{.*?\s+(?P<package>[^\s]+)/(?P<activity>[^\s]+)\s.*?\}')
        m = _recordRE.search(output)
        if m:
            package = m.group("package")

        # try: adb shell dumpsys activity top
        _activityRE = re.compile(
            r'ACTIVITY (?P<package>[^\s]+)/(?P<activity>[^/\s]+) \w+ pid=(?P<pid>\d+)'
        )
        output = self.shell(['dumpsys', 'activity', 'top'])
        ms = _activityRE.finditer(output)
        ret = None
        for m in ms:
            ret = RunningAppInfo(package=m.group('package'),
                                 activity=m.group('activity'),
                                 pid=int(m.group('pid')))
            if ret.package == package:
                return ret

        if ret:  # get last result
            return ret
        raise AdbError("Couldn't get focused app")

    def remove(self, path: str):
        """ rm device file """
        self.shell(["rm", path])


    def screenrecord(self, remote_path=None, no_autostart=False):
        """
        Args:
            remote_path: device video path
            no_autostart: do not start screenrecord, when call this method
        """
        if self.shell2("which screenrecord").returncode != 0:
            raise AdbError("screenrecord command not found")
        return _ScreenRecord(self, remote_path, autostart=not no_autostart)

    def _start_recording(self, h264_filename: str):
        """ start video recording """
        return self._scrcpy.start_recording(h264_filename)
    
    def _stop_recording(self):
        """ stop video recording """
        return self._scrcpy.stop_recording()
    
    def _is_recording(self) -> bool:
        return self._scrcpy.running

    @property
    def _scrcpy(self) -> "ScrcpyClient":
        return ScrcpyClient.get_instance(self)


class ScrcpyClient:
    """
    # -y  overwrite output files
    ffmpeg -i "output.h264" -c:v copy -f mp4 -y "video.mp4"

    遗留问题：秒表视频，转化后的视频时长不对啊（本来5s的，转化后变成了20s）

    https://stackoverflow.com/questions/21263064/how-to-wrap-h264-into-a-mp4-container
    """
    __instances = {}

    def __init__(self, d: AdbDevice, h264_filename: str = None):
        self._d = d
        self._filename = h264_filename
        self._conn: AdbConnection = None
        self._done_event = threading.Event()

    @classmethod
    def get_instance(cls, d: AdbDevice) -> "ScrcpyClient":
        if d in cls.__instances:
            return cls.__instances[d]
        c = cls.__instances[d] = ScrcpyClient(d)
        return c
    
    @property
    def running(self) -> bool:
        return self._conn and not self._conn.closed
    
    def start_recording(self, h264_filename: str):
        if self.running:
            raise RuntimeError("already running")
        self._filename = h264_filename
        self._start()

    def stop_recording(self):
        if not self.running:
            raise RuntimeError("already stopped")
        self._stop()

    def _start(self):
        curdir = pathlib.Path(__file__).absolute().parent
        device_jar_path = "/data/local/tmp/scrcpy-server.jar"
        scrcpy_server_jar_path = curdir.joinpath("binaries/scrcpy-server-1.24.jar")

        self._d.sync.push(scrcpy_server_jar_path, device_jar_path)

        opts = [
            'control=false', 'bit_rate=8000000', 'tunnel_forward=true',
            'lock_video_orientation=-1', 'send_dummy_byte=false',
            "send_device_meta=false", "send_frame_meta=true", "downsize_on_error=true"
        ]
        cmd = [
            'CLASSPATH=' + device_jar_path, 'app_process', '/',
            '--nice-name=scrcpy-server', 'com.genymobile.scrcpy.Server', '1.24'
        ] + opts
        _c = self._d.shell(cmd, stream=True)
        c: AdbConnection = _c
        del(_c)
        message = c.conn.recv(100).decode('utf-8')
        print("Scrcpy:", message)
        self._conn = c
        threading.Thread(name="scrcpy_main", target=self._copy2null, args=(c.conn,), daemon=True).start()
        time.sleep(0.1)
        stream_sock = self._safe_dial_scrcpy()
        fh = pathlib.Path(self._filename).open("wb")
        threading.Thread(name="socket_copy", target=self._copy2file, args=(stream_sock, fh), daemon=True).start()

    @retry(AdbError, tries=10, delay=0.1, jitter=0.01)
    def _safe_dial_scrcpy(self) -> socket.socket:
        return self._d.create_connection(Network.LOCAL_ABSTRACT, "scrcpy")

    def _copy2null(self, s: socket.socket):
        while True:
            try:
                chunk = s.recv(1024)
                if chunk == b"":
                    print("O:", chunk.decode('utf-8'))
                    break
            except:
                break
        print("Scrcpy mainThread stopped")

    def _copy2file(self, s: socket.socket, fh: typing.BinaryIO):
        while True:
            chunk = s.recv(1<<16)
            if not chunk:
                break
            fh.write(chunk)
        fh.close()
        print("Copy h264 stream finished", flush=True)
        self._done_event.set()
    
    def _stop(self) -> bool:
        self._conn.close()
        self._done_event.wait(timeout=3.0)
        time.sleep(1)
        self._done_event.clear()


class _ScreenRecord():
    def __init__(self, d: AdbDevice, remote_path=None, autostart=False):
        """ The maxium record time is 3 minutes """
        self._d = d
        if not remote_path:
            remote_path = "/sdcard/video-%d.mp4" % int(time.time() * 1000)
        self._remote_path = remote_path
        self._stream = None
        self._stopped = False
        self._started = False

        if autostart:
            self.start()

    def start(self):
        """ start recording """
        if self._started:
            warnings.warn("screenrecord already started", UserWarning)
            return
        # end first line with \ to avoid the empty line!
        script_content = textwrap.dedent("""\
        #!/system/bin/sh
        # generate by adbutils
        screenrecord "$1" &
        PID=$!
        read ANY
        kill -INT $PID
        wait
        """).encode('utf-8')
        self._d.sync.push(script_content, "/sdcard/adbutils-screenrecord.sh")
        self._stream: AdbConnection = self._d.shell(["sh", "/sdcard/adbutils-screenrecord.sh", self._remote_path],
                                     stream=True)
        self._started = True

    def stop(self):
        """ stop recording """
        if not self._started:
            raise RuntimeError("screenrecord is not started")

        if self._stopped:
            return
        self._stream.send(b"\n")
        self._stream.read_until_close()
        self._stream.close()
        self._stopped = True

    def stop_and_pull(self, path: typing.Union[str, pathlib.Path]):
        """ pull remote to local and remove remote file """
        if isinstance(path, pathlib.Path):
            path = path.as_posix()
        self.stop()
        self._d.sync.pull(self._remote_path, path)
        self._d.remove(self._remote_path)