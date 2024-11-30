# coding: utf-8
# Author: codeskyblue 2018-06-04

import argparse
import ctypes
import os
import pathlib
import sys
import tkinter as tk
import winreg

import apkutils


def _bind_apk_right_menu():
    if not ctypes.windll.shell32.IsUserAnAdmin():
        ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, __file__ + " --bind", None, 0)
        return

    with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, r"*\shell") as key:
        print(key)
        with winreg.CreateKeyEx(key, "APK Parser", 0, winreg.KEY_SET_VALUE) as shell_key:
            icon_path = str(pathlib.Path(__file__).joinpath(
                "../android.ico").resolve())
            winreg.SetValueEx(shell_key, "Icon", 0, winreg.REG_SZ, icon_path)
            with winreg.CreateKey(shell_key, "command") as cmd_key:
                winreg.SetValue(
                    cmd_key, "", 1, " ".join(
                        [sys.executable.replace("python.exe", "pythonw.exe"), os.path.abspath(__file__), "--file",  "\"%1\""]))


def _unbind_reg():
    if not ctypes.windll.shell32.IsUserAnAdmin():
        ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, __file__ + "  --unbind", None, 0)
        return
    try:
        winreg.DeleteKey(winreg.HKEY_LOCAL_MACHINE,
                         r"SOFTWARE\Classes\*\shell\APK Parser\command")
        winreg.DeleteKey(winreg.HKEY_LOCAL_MACHINE,
                         r"SOFTWARE\Classes\*\shell\APK Parser")
    except FileNotFoundError:
        pass


class TKList(object):
    def __init__(self):
        self._row = 0

    def add_row(self, *widgets):
        for (column, widget) in enumerate(widgets):
            if isinstance(widget, str):
                text = tk.Text(height=1)
                text.insert(tk.INSERT, widget)

                # ref: https://tkdocs.com/tutorial/text.html
                text.bind("<FocusIn>", lambda event: text.tag_add(
                    tk.SEL, "1.0", "1.end"))
                widget = text
            widget.grid(row=self._row, column=column)
        self._row += 1


def main(path):
    root = tk.Tk()

    if path:
        apk = apkutils2.APK(path)
        mf = apk.manifest
        grid = TKList()
        grid.add_row(tk.Label(text="Filename"), os.path.basename(path))
        grid.add_row(tk.Label(text="Package Name"), mf.package_name)
        grid.add_row(tk.Label(text="Main Activity"), mf.main_activity)
        grid.add_row(tk.Label(text="Version Name"), mf.version_name)
        grid.add_row(tk.Label(text="Version Code"), mf.version_code)
    else:
        tk.Button(root, text="Bind to *.apk Right MENU",
                  command=_bind_apk_right_menu).pack(padx=10, pady=5)
        tk.Button(root, text="Unbind",
                  command=_unbind_reg).pack(padx=10, pady=5, side=tk.LEFT)

    tk._default_root.title("APK Parser")
    tk.mainloop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="parsed file")
    parser.add_argument("--bind", action="store_true",
                        help="Bind right-click menu")
    parser.add_argument("--unbind", action="store_true",
                        help="Unbind right-click menu")
    args = parser.parse_args()

    if args.bind:
        _bind_apk_right_menu()
    elif args.unbind:
        _unbind_reg()
    else:
        main(args.file)
