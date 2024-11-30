# coding: utf-8
#
"""
python -m apkutils COMMAND

Commands:
    install     Install apk to device
"""

from __future__ import absolute_import

import argparse
import datetime
import functools
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile

import requests

import adbutils
from adbutils import adb as adbclient
from adbutils._utils import ReadProgress, current_ip, APKReader


def _setup_minicap(d: adbutils.AdbDevice):
    def cache_download(url, dst):
        if os.path.exists(dst):
            print("Use cached", dst)
            return
        print("Download {} from {}".format(dst, url))
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        length = int(resp.headers.get("Content-Length", 0))
        r = ReadProgress(resp.raw, length)
        with open(dst + ".cached", "wb") as f:
            shutil.copyfileobj(r, f)
        shutil.move(dst + ".cached", dst)

    def push_zipfile(path: str,
                     dest: str,
                     mode=0o755,
                     zipfile_path: str = "vendor/stf-binaries-master.zip"):
        """ push minicap and minitouch from zip """
        with zipfile.ZipFile(zipfile_path) as z:
            if path not in z.namelist():
                print("WARNING: stf stuff %s not found", path)
                return
            with z.open(path) as f:
                d.sync.push(f, dest, mode)

    zipfile_path = "stf-binaries.zip"
    cache_download("https://github.com/openatx/stf-binaries/archive/0.2.zip",
                   zipfile_path)
    zip_folder = "stf-binaries-0.2"

    sdk = d.getprop("ro.build.version.sdk")  # eg 26
    abi = d.getprop('ro.product.cpu.abi')  # eg arm64-v8a
    abis = (d.getprop('ro.product.cpu.abilist').strip() or abi).split(",")
    # return
    print("sdk: %s, abi: %s, support-abis: %s" % (sdk, abi, ','.join(abis)))
    print("Push minicap+minicap.so to device")
    prefix = zip_folder + "/node_modules/minicap-prebuilt/prebuilt/"
    push_zipfile(prefix + abi + "/lib/android-" + sdk + "/minicap.so",
                 "/data/local/tmp/minicap.so", 0o644, zipfile_path)
    push_zipfile(prefix + abi + "/bin/minicap", "/data/local/tmp/minicap",
                 0o0755, zipfile_path)

    print("Push minitouch to device")
    prefix = zip_folder + "/node_modules/minitouch-prebuilt/prebuilt/"
    push_zipfile(prefix + abi + "/bin/minitouch", "/data/local/tmp/minitouch",
                 0o0755, zipfile_path)

    # check if minicap installed
    output = d.shell(
        ["LD_LIBRARY_PATH=/data/local/tmp", "/data/local/tmp/minicap", "-i"])
    print(output)
    print("If you see JSON output, it means minicap installed successfully")


def main():
    parser = argparse.ArgumentParser()
    # formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-s", "--serial", help="device serial number")
    parser.add_argument("-V",
                        "--server-version",
                        action="store_true",
                        help="show adb server version")
    parser.add_argument("-l",
                        "--list",
                        action="store_true",
                        help="list devices")
    parser.add_argument("-i",
                        "--install",
                        help="install from local apk or url")
    parser.add_argument(
        "--install-confirm",
        action="store_true",
        help="auto confirm when install (based on uiautomator2)")
    parser.add_argument("-u", "--uninstall", help="uninstall apk")
    parser.add_argument("-L", "--launch", action="store_true", help="launch after install")
    parser.add_argument("--qrcode", help="show qrcode of the specified file")
    parser.add_argument("--parse", type=str, help="parse package info from local file or url")
    parser.add_argument("--clear",
                        action="store_true",
                        help="clear all data when uninstall")
    parser.add_argument("--list-packages",
                        action="store_true",
                        help="list packages installed")
    parser.add_argument("--current", action="store_true", help="show current package info")
    parser.add_argument("-p",
                        "--package",
                        help="show package info in json format")
    parser.add_argument("--grep", help="filter matched package names")
    parser.add_argument("--connect", type=str, help="connect remote device")
    parser.add_argument("--shell",
                        action="store_true",
                        help="run shell command")
    parser.add_argument("--minicap",
                        action="store_true",
                        help="install minicap and minitouch to device")
    parser.add_argument("--screenshot", type=str, help="take screenshot")
    parser.add_argument("-b", "--browser", help="open browser in device")
    parser.add_argument(
        "--push",
        help=
        "push local to remote, arg is colon seperated, eg some.txt:/sdcard/s.txt"
    )
    parser.add_argument(
        "--pull",
        help="push local to remote, arg is colon seperated, eg /sdcard/some.txt"
    )
    parser.add_argument("--dump-info", action="store_true", help="dump info for developer")
    parser.add_argument("--track", action="store_true", help="trace device status")
    parser.add_argument("args", nargs="*", help="arguments")

    args = parser.parse_args()

    if args.connect:
        adbclient.connect(args.connect)
        return

    if args.server_version:
        print("ADB Server version: {}".format(adbclient.server_version()))
        return

    if args.list:
        rows = []
        for d in adbclient.device_list():
            rows.append([d.serial, d.shell("getprop ro.product.model")])
        lens = []
        for col in zip(*rows):
            lens.append(max([len(v) for v in col]))
        format = "  ".join(["{:<" + str(l) + "}" for l in lens])
        for row in rows:
            print(format.format(*row))
        return

    if args.qrcode:
        from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

        filename = args.qrcode
        port = 8000
        url = "http://%s:%d/%s" % (current_ip(), port, filename)
        print("File URL:", url)
        try:
            import qrcode
            qr = qrcode.QRCode(border=2)
            qr.add_data(url)
            qr.print_ascii(tty=True)
        except ImportError:
            print(
                "In order to show QRCode, you need install with: pip3 install qrcode"
            )

        httpd = ThreadingHTTPServer(('', port), SimpleHTTPRequestHandler)
        httpd.serve_forever()
        return

    if args.dump_info:
        print("==== ADB Info ====")
        print("Path:", adbutils.adb_path())
        print("Server version:", adbclient.server_version())
        print("")
        print(">> List of devices attached")
        for d in adbclient.device_list():
            print("-", d.serial, d.prop.name, d.prop.model)
        return
    
    if args.track:
        for event in adbclient.track_devices():
            asctime = datetime.datetime.now().strftime("%H:%M:%S.%f")
            print("{} {} -> {}".format(asctime[:-3], event.serial, event.status))
        return

    elif args.parse:
        uri = args.parse
        
        fp = None
        if re.match(r"^https?://", uri):
            try:
                import httpio
            except ImportError:
                retcode = subprocess.call([sys.executable, '-m', 'pip', 'install', '-U', 'httpio'])
                assert retcode == 0
                import httpio
            fp = httpio.open(uri, block_size=-1)
        else:
            assert os.path.isfile(uri)
            fp = open(uri, 'rb')
        try:
            ar = APKReader(fp)
            ar.dump_info()
        finally:
            fp.close()
        return

    ## Device operation
    d = adbclient.device(args.serial)

    if args.shell:
        output = d.shell(args.args)
        print(output)
        return

    if args.install:
        def _callback(event_name: str, ud):
            name = "_INSTALL_"
            if event_name == "BEFORE_INSTALL":
                print("== Enable popup window watcher")
                ud.press("home")
                ud.watcher(name).when("允许").click()
                ud.watcher(name).when("继续安装").click()
                ud.watcher(name).when("安装").click()
                ud.watcher.start()
            elif event_name == "FINALLY":
                print("== Stop popup window watcher")
                ud.watcher.remove(name)
                ud.watcher.stop()
        
        if args.install_confirm:
            import uiautomator2 as u2
            ud = u2.connect(args.serial)
            _callback = functools.partial(_callback, ud=ud)
        else:
            _callback = None

        d.install(args.install, uninstall=True, callback=_callback)

    elif args.uninstall:
        d.uninstall(args.uninstall)

    elif args.list_packages:
        patten = re.compile(args.grep or ".*")
        for p in d.list_packages():
            if patten.search(p):
                print(p)

    elif args.screenshot:
        if args.minicap:
            def adb_shell(cmd: list):
                print("Run:", " ".join(["adb", "shell"] + cmd))
                return d.shell(cmd).strip()
            json_output = adb_shell([
                "LD_LIBRARY_PATH=/data/local/tmp", "/data/local/tmp/minicap",
                "-i", "2&>/dev/null"
            ])
            if not json_output.startswith("{"):
                raise RuntimeError("Invalid json format", json_output)
            data = json.loads(json_output)
            
            w, h, r = data["width"], data["height"], data["rotation"]
            d.shell([
                "LD_LIBRARY_PATH=/data/local/tmp", "/data/local/tmp/minicap",
                "-P", "{0}x{1}@{0}x{1}/{2}".format(w, h, r), "-s",
                ">/sdcard/minicap.jpg"
            ])
            d.sync.pull("/sdcard/minicap.jpg", args.screenshot)
        else:
            remote_tmp_path = "/data/local/tmp/screenshot.png"
            d.shell(["rm", remote_tmp_path])
            d.shell(["screencap", "-p", remote_tmp_path])
            d.sync.pull(remote_tmp_path, args.screenshot)

    elif args.minicap:  # without args.screenshot
        _setup_minicap(d)

    elif args.push:
        local, remote = args.push.split(":", 1)
        length = os.stat(local).st_size
        with open(local, "rb") as fd:
            r = ReadProgress(fd, length)
            d.sync.push(r, remote, filesize=length)

    elif args.pull:
        remote_path = args.pull
        target_path = os.path.basename(remote_path)
        finfo = d.sync.stat(args.pull)

        if finfo.mode == 0 and finfo.size == 0:
            sys.exit(f"remote file '{remote_path}' does not exist")

        bytes_so_far = 0
        for chunk in d.sync.iter_content(remote_path):
            bytes_so_far += len(chunk)
            percent = bytes_so_far / finfo.size * 100 if finfo.size != 0 else 100.0
            print(
                f"\rDownload to {target_path} ... [{bytes_so_far} / {finfo.size}] %.1f %%"
                % percent,
                end="",
                flush=True)
        print(f"{remote_path} pulled to {target_path}")

    elif args.browser:
        d.open_browser(args.browser)

    elif args.current:
        package_name = d.app_current().package
        info = d.package_info(package_name)
        print(json.dumps(info, indent=4, default=str))

    elif args.package:
        info = d.package_info(args.package)
        print(json.dumps(info, indent=4, default=str))



if __name__ == "__main__":
    main()
