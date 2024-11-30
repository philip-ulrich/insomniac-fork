# coding: utf-8
#

from __future__ import print_function

import io
import os
import typing

from deprecation import deprecated

from ._adb import AdbConnection
from ._adb import BaseClient as _BaseClient
from ._device import AdbDevice, Sync
from ._proto import *
from ._utils import adb_path
from ._version import __version__
from .errors import *


class AdbClient(_BaseClient):
    def sync(self, serial: str) -> Sync:
        return Sync(self, serial)

    @deprecated(deprecated_in="0.15.0",
                removed_in="1.0.0",
                current_version=__version__,
                details="use AdbDevice.shell instead")
    def shell(self,
              serial: str,
              command: typing.Union[str, list, tuple],
              stream: bool = False,
              timeout: typing.Optional[float] = None) -> typing.Union[str, AdbConnection]:
        return self.device(serial).shell(command, stream=stream, timeout=timeout)

    def iter_device(self) -> typing.Iterator:
        """
        Returns:
            iter of AdbDevice
        """
        with self._connect() as c:
            c.send_command("host:devices")
            c.check_okay()
            output = c.read_string_block()
            for line in output.splitlines():
                parts = line.strip().split("\t")
                if len(parts) != 2:
                    continue
                if parts[1] == 'device':
                    yield AdbDevice(self, serial=parts[0])

    def device_list(self):
        return list(self.iter_device())

    def device(self,
               serial: str = None,
               transport_id: int = None) -> 'AdbDevice':
        if serial:
            return AdbDevice(self, serial=serial)
        
        if transport_id:
            return AdbDevice(self, transport_id=transport_id)

        serial = os.environ.get("ANDROID_SERIAL")
        if not serial:
            ds = self.device_list()
            if len(ds) == 0:
                raise RuntimeError("Can't find any android device/emulator")
            if len(ds) > 1:
                raise RuntimeError(
                    "more than one device/emulator, please specify the serial number"
                )
            return ds[0]
        return AdbDevice(self, serial)



adb = AdbClient()
device = adb.device


if __name__ == "__main__":
    print("server version:", adb.server_version())
    print("devices:", adb.devices())
    d = adb.devices()[0]

    print(d.serial)
    for f in adb.sync(d.serial).iter_directory("/data/local/tmp"):
        print(f)

    finfo = adb.sync(d.serial).stat("/data/local/tmp")
    print(finfo)
    import io
    sync = adb.sync(d.serial)
    filepath = "/data/local/tmp/hi.txt"
    sync.push(io.BytesIO(b"hi5a4de5f4qa6we541fq6w1ef5a61f65ew1rf6we"),
              filepath, 0o644)

    print("FileInfo", sync.stat(filepath))
    for chunk in sync.iter_content(filepath):
        print(chunk)
    # sync.pull(filepath)
