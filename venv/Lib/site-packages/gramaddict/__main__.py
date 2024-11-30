import argparse
from os import getcwd, path

from GramAddict import __version__
from GramAddict.core.bot_flow import start_bot
from GramAddict.core.download_from_github import download_from_github


def cmd_init(args):
    if args.account_name is not None:
        print(f"Script launched in {getcwd()}, files will be available there.")
        for username in args.account_name:
            if not path.exists("./run.py"):
                print("Creating run.py ...")
                download_from_github(
                    "https://github.com/GramAddict/bot/blob/master/run.py"
                )
            if not path.exists(f"./accounts/{username}"):
                print(
                    f"Creating 'accounts/{username}' folder with a config starting point inside. You have to edit these files according with https://docs.gramaddict.org/#/configuration"
                )
                download_from_github(
                    "https://github.com/GramAddict/bot/tree/master/config-examples",
                    output_dir=f"accounts/{username}",
                    flatten=True,
                )
            else:
                print(f"'accounts/{username}' folder already exists, skip.")
                continue
            with open(f"./accounts/{username}/config.yml", "r+", encoding="utf-8") as f:
                config = f.read()
                f.seek(0)
                config_fixed = config.replace("myusername", username)
                f.write(config_fixed)
    else:
        print("You have to provide at last one account name..")


def cmd_run(args):
    start_bot()


def cmd_dump(args):
    import os
    import shutil
    import time

    import uiautomator2 as u2
    from colorama import Fore, Style

    if not args.no_kill:
        os.popen("adb shell pkill atx-agent").close()
    try:
        d = u2.connect(args.device)
    except RuntimeError as err:
        raise SystemExit(err)

    def dump_hierarchy(device, path):
        xml_dump = device.dump_hierarchy()
        with open(path, "w", encoding="utf-8") as outfile:
            outfile.write(xml_dump)

    def make_archive(name):
        os.chdir("dump")
        shutil.make_archive(base_name=f"screen_{name}", format="zip", root_dir="cur")
        shutil.rmtree("cur")

    os.makedirs("dump/cur", exist_ok=True)
    d.screenshot("dump/cur/screenshot.png")
    dump_hierarchy(d, "dump/cur/hierarchy.xml")
    archive_name = int(time.time())
    make_archive(archive_name)
    print(
        Fore.GREEN
        + Style.BRIGHT
        + "\nCurrent screen dump generated successfully! Please, send me this file:"
    )
    print(Fore.BLUE + Style.BRIGHT + f"{os.getcwd()}\\screen_{archive_name}.zip")


_commands = [
    dict(
        action=cmd_init,
        command="init",
        help="creates your account folder under accounts with files for configuration",
        flags=[
            dict(
                args=["account_name"],
                nargs="+",
                help="instagram account name to initialize",
            ),
        ],
    ),
    dict(
        action=cmd_run,
        command="run",
        help="start the bot!",
        flags=[
            dict(args=["--config"], nargs="?", help="provide the config.yml path"),
        ],
    ),
    dict(
        action=cmd_dump,
        command="dump",
        help="dump current screen",
        flags=[
            dict(
                args=["--device"],
                nargs=None,
                default=None,
                help="provide the device name if more then one connected",
            ),
            dict(
                args=["--no-kill"],
                action="store_true",
                help="don't kill the uia2 demon",
            ),
        ],
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="GramAddict",
        description="free human-like Instagram bot",
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"{parser.prog} {__version__}"
    )
    subparser = parser.add_subparsers(dest="subparser")
    actions = {}
    for c in _commands:
        cmd_name = c["command"]
        actions[cmd_name] = c["action"]
        sp = subparser.add_parser(
            cmd_name,
            help=c.get("help"),
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        for f in c.get("flags", []):
            args = f.get("args")
            if not args:
                args = ["-" * min(2, len(n)) + n for n in f["name"]]
            kwargs = f.copy()
            kwargs.pop("name", None)
            kwargs.pop("args", None)
            kwargs.pop("run", None)
            sp.add_argument(*args, **kwargs)

    args = parser.parse_args()

    if args.subparser:
        actions[args.subparser](args)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
