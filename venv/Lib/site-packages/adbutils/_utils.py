import hashlib
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import typing
import zipfile

import whichcraft
from apkutils2.axml.axmlparser import AXML
from apkutils2.manifest import Manifest
from pkg_resources import resource_filename

MB = 1024 * 1024

def humanize(n: int) -> str:
    return '%.1f MB' % (float(n) / MB)


def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    try:
        return s.getsockname()[1]
    finally:
        s.close()


def list2cmdline(args: typing.Union[list, tuple]):
    """ do not use subprocess.list2cmdline, use this instead """
    return ' '.join(map(shlex.quote, args))


def current_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        return ip
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()

def adb_path():
    # 0. check env: ADBUTILS_ADB_PATH
    if os.getenv("ADBUTILS_ADB_PATH"):
        return os.getenv("ADBUTILS_ADB_PATH")
        
    # 1. find in $PATH
    exe = whichcraft.which("adb")
    if exe and _is_valid_exe(exe):
        return exe
    
    # 2. use buildin adb
    bin_dir = resource_filename("adbutils", "binaries")
    exe = os.path.join(bin_dir, "adb.exe" if os.name == 'nt' else 'adb')
    if os.path.isfile(exe) and _is_valid_exe(exe):
        return exe

    raise RuntimeError("No adb exe could be found. Install adb on your system")


def _popen_kwargs(prevent_sigint=False):
    startupinfo = None
    preexec_fn = None
    creationflags = 0
    if sys.platform.startswith("win"):
        # Stops executable from flashing on Windows (see imageio/imageio-ffmpeg#22)
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    if prevent_sigint:
        # Prevent propagation of sigint (see imageio/imageio-ffmpeg#4)
        # https://stackoverflow.com/questions/5045771
        if sys.platform.startswith("win"):
            creationflags = 0x00000200
        else:
            preexec_fn = os.setpgrp  # the _pre_exec does not seem to work
    return {
        "startupinfo": startupinfo,
        "creationflags": creationflags,
        "preexec_fn": preexec_fn,
    }


def _is_valid_exe(exe: str):
    cmd = [exe, "version"]
    try:
        subprocess.check_call(
            cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT, **_popen_kwargs()
        )
        return True
    except (OSError, ValueError, subprocess.CalledProcessError):
        return False


class ReadProgress():
    def __init__(self, r, total_size: int, source_path=None):
        """
        Args:
            source_path (str): store read content to filepath
        """
        self.r = r
        self.total = total_size
        self.copied = 0
        self.start_time = time.time()
        self.update_time = time.time()
        self.m = hashlib.md5()
        self._chunk_size = 0
        self._hash = ''
        self._tmpfd = None if source_path else tempfile.NamedTemporaryFile(suffix=".apk")
        self._filepath = source_path

    def update(self, chunk: bytes):
        chunk_size = len(chunk)
        self.m.update(chunk)
        if chunk_size == 0:
            self._hash = self.m.hexdigest()
        self.copied += chunk_size
        self._chunk_size += chunk_size

        if self.total:
            percent = float(self.copied) / self.total * 100
        else:
            percent = 0.0 if chunk_size else 100.0

        p = int(percent)
        duration = time.time() - self.update_time
        if p == 100.0 or duration > 1.0:
            if duration:
                speed = humanize(self._chunk_size / duration) + "/s"
            else:
                copytime = max(0.1, time.time() - self.start_time)
                speed = humanize(self.copied / copytime) + "/s"

            self.update_time = time.time()
            self._chunk_size = 0

            copysize = humanize(self.copied)
            totalsize = humanize(self.total)
            print("{:.1f}%\t{} [{}/{}]".format(percent, speed, copysize,
                                               totalsize))

    def read(self, n: int) -> bytes:
        chunk = self.r.read(n)
        self.update(chunk)
        if self._tmpfd:
            self._tmpfd.write(chunk)
        return chunk
    
    def filepath(self):
        if self._filepath:
            return self._filepath
        self._tmpfd.seek(0)
        return self._tmpfd.name


class APKReader:
    def __init__(self, fp: typing.BinaryIO):
        self._fp = fp
    
    def dump_info(self):
        zf = zipfile.ZipFile(self._fp)
        raw_manifest = zf.read("AndroidManifest.xml")
        axml = AXML(raw_manifest)
        if not axml.is_valid:
            print("axml is invalid")
            return
        am = Manifest(axml.get_xml())
        print("package:", am.package_name)
        print("main-activity:", am.main_activity)
        print("version-name:", am.version_name)
        print("version-code:", am.version_code)
