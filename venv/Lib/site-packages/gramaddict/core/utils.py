import logging
import os
import random
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from math import nan
from os import getcwd, rename, walk
from pathlib import Path
from random import randint, shuffle, uniform
from subprocess import PIPE
from time import sleep
from typing import Optional, Tuple, Union
from urllib.parse import urlparse

import emoji
import requests
import uiautomator2.exceptions
import urllib3
from colorama import Fore, Style
from packaging.version import parse as parse_version

from GramAddict import __file__, __version__
from GramAddict.core.config import Config
from GramAddict.core.log import get_log_file_config
from GramAddict.core.report import print_full_report
from GramAddict.core.resources import ResourceID as resources
from GramAddict.core.storage import ACCOUNTS

http = urllib3.PoolManager()
logger = logging.getLogger(__name__)


def load_config(config: Config):
    global app_id
    global args
    global configs
    global ResourceID
    app_id = config.args.app_id
    args = config.args
    configs = config
    ResourceID = resources(app_id)


def update_available():
    response = requests.get("https://pypi.python.org/pypi/gramaddict/json")
    if response.ok:
        latest_version = response.json()["info"]["version"]

        current_version = parse_version(__version__)
        latest_version = parse_version(latest_version)

        return current_version < latest_version, latest_version
    else:
        return False, None


def check_if_updated(crash=False):
    if not crash:
        logger.info("Checking for updates...")
    new_update, latest_version = update_available()
    if new_update:
        logger.warning("NEW VERSION FOUND!")
        logger.warning(
            f"Version {latest_version} has been released! Please update so that you can get all the latest features and bugfixes. Changelog here -> https://github.com/GramAddict/bot/blob/master/CHANGELOG.md"
        )
        logger.warning("HOW TO UPDATE:")
        logger.warning("If you installed with pip: pip3 install GramAddict -U")
        logger.warning("If you installed with git: git pull")
        sleep(5)
    elif latest_version is None:
        logger.error("Unable to get latest version from pypi!")
    elif not crash:
        logger.info("Bot is updated.", extra={"color": f"{Style.BRIGHT}"})

    if not crash:
        logger.info(
            f"GramAddict v.{__version__}",
            extra={"color": f"{Style.BRIGHT}{Fore.MAGENTA}"},
        )


def ask_for_a_donation():
    logger.info(
        "This bot is backed with love by me for free. If you like using it, consider donating to help keep me motivated: https://www.buymeacoffee.com/mastrolube",
        extra={"color": f"{Style.BRIGHT}{Fore.MAGENTA}"},
    )


def move_usernames_to_accounts():
    Path(ACCOUNTS).mkdir(parents=True, exist_ok=True)
    ls = next(walk("."))[1]
    ignored_dir = [
        "__pycache__",
        "build",
        "accounts",
        "GramAddict",
        "config-examples",
        ".git",
        ".venv",
        "dist",
        ".vscode",
        ".github",
        "crashes",
        "gramaddict.egg-info",
        "logs",
        "res",
        "test",
        "dump",
    ]
    for n in ignored_dir:
        try:
            ls.remove(n)
        except ValueError:
            pass

    for dir in ls:
        try:
            if dir != dir.strip():
                rename(f"{dir}", dir.strip())
            shutil.move(dir.strip(), ACCOUNTS)
        except Exception as e:
            logger.error(
                f"Folder {dir.strip()} already exists! Won't overwrite it, please check which is the correct one and delete the other! Exception: {e}"
            )
            sleep(3)
    if len(ls) > 0:
        logger.warning(
            f"Username folders {', '.join(ls)} have been moved to main folder 'accounts'. Remember that your config file must point there! Example: '--config accounts/yourusername/config.yml'"
        )


def config_examples():
    if getcwd() == __file__[:-23]:
        logger.debug("Installed via git, config-examples is in the local folder.")
    else:
        logger.debug("Installed via pip.")
        logger.info(
            "Do you want to update/create your config-examples folder in local? Do the following: \n\t\t\t\tpip3 install --user gitdir (only the first time)\n\t\t\t\tpython3 -m gitdir https://github.com/GramAddict/bot/tree/master/config-examples (python on Windows)",
            extra={"color": Fore.GREEN},
        )
        sleep(3)


def check_adb_connection():
    is_device_id_provided = configs.device_id is not None
    # sometimes it needs two requests to wake up...
    stream = os.popen("adb devices")
    stream.close()
    stream = os.popen("adb devices")
    output = stream.read()
    devices_count = len(re.findall("device\n", output))
    stream.close()

    is_ok = True
    message = "That's ok."
    if devices_count == 0:
        is_ok = False
        message = "Cannot proceed."
    elif devices_count > 1 and not is_device_id_provided:
        is_ok = False
        message = "Set a device name in your config.yml"

    if is_ok:
        logger.debug(f"Connected devices via adb: {devices_count}. {message}")
    else:
        logger.error(f"Connected devices via adb: {devices_count}. {message}")

    return is_ok


def get_instagram_version():
    stream = os.popen(
        f"adb{'' if configs.device_id is None else ' -s ' + configs.device_id} shell dumpsys package {app_id}"
    )
    output = stream.read()
    version_match = re.findall("versionName=(\\S+)", output)
    version = version_match[0] if len(version_match) == 1 else "not found"
    stream.close()
    return version


def open_instagram_with_url(url) -> bool:
    logger.info(f"Open Instagram app with url: {url}")
    cmd = f"adb{'' if configs.device_id is None else ' -s ' + configs.device_id} shell am start -a android.intent.action.VIEW -d {url}"
    cmd_res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")
    err = cmd_res.stderr.strip()
    random_sleep()
    if err:
        logger.debug(err)
        return False
    return True


def kill_app(device, app_id):
    device.deviceV2.app_stop(app_id)


def head_up_notifications(enabled: bool = False):
    """
    Enable or disable head-up-notifications
    """
    cmd: str = (
        f"adb{'' if configs.device_id is None else ' -s ' + configs.device_id} shell settings put global heads_up_notifications_enabled {0 if not enabled else 1}"
    )
    return subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")


def check_screen_timeout():
    MIN_TIMEOUT = 5 * 6_000
    cmd: str = (
        f"adb{'' if configs.device_id is None else f' -s {configs.device_id}'} shell settings get system screen_off_timeout"
    )
    resp = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")
    try:
        if int(resp.stdout.lstrip()) < MIN_TIMEOUT:
            logger.info(
                f"Setting timeout of the screen to {MIN_TIMEOUT/6_000:.0f} minutes."
            )
            cmd: str = (
                f"adb{'' if configs.device_id is None else f' -s {configs.device_id}'} shell settings put system screen_off_timeout {MIN_TIMEOUT}"
            )

            subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")
        else:
            logger.info("Screen timeout is fine!")
    except ValueError:
        logger.info("Unable to get screen timeout!")
        logger.debug(resp.stdout)


def open_instagram(device):
    nl = "\n"
    FastInputIME = "com.github.uiautomator/.FastInputIME"
    logger.info("Open Instagram app.")

    def call_ig():
        try:
            return device.deviceV2.app_start(app_id, use_monkey=True)
        except uiautomator2.exceptions.BaseError as exc:
            return exc

    err = call_ig()
    if err:
        logger.error(err)
        return False
    else:
        logger.debug("Instagram called successfully.")

    max_tries = 3
    n = 0
    while device.deviceV2.app_current()["package"] != app_id:
        if n == max_tries:
            logger.critical(
                f"Unable to open Instagram. Bot will stop. Current package name: {device.deviceV2.app_current()['package']} (Looking for {app_id})"
            )
            return False
        n += 1
        logger.info(f"Waiting for Instagram to open... 😴 ({n}/{max_tries})")
        if check_if_crash_popup_is_there(device):
            logger.info("Ig crashed, try to open it again...")
        call_ig()
        choose_cloned_app(device)
        random_sleep(3, 3, modulable=False)

    logger.info("Ready for botting!🤫", extra={"color": f"{Style.BRIGHT}{Fore.GREEN}"})

    random_sleep()
    if configs.args.close_apps:
        logger.info("Close all the other apps, to avoid interferences...")
        device.deviceV2.app_stop_all(excludes=[app_id])
        random_sleep()
    logger.debug("Setting FastInputIME as default keyboard.")
    device.deviceV2.set_fastinput_ime(True)
    cmd: str = (
        f"adb{'' if configs.device_id is None else ' -s ' + configs.device_id} shell settings get secure default_input_method"
    )
    cmd_res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")
    if cmd_res.stdout.replace(nl, "") != FastInputIME:
        logger.warning(
            f"FastInputIME is not the default keyboard! Default is: {cmd_res.stdout.replace(nl, '')}. Changing it via adb.."
        )
        cmd: str = (
            f"adb{'' if configs.device_id is None else ' -s ' + configs.device_id} shell ime set {FastInputIME}"
        )
        cmd_res = subprocess.run(
            cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8"
        )
        if cmd_res.stdout.startswith("Error:"):
            logger.warning(
                f"{cmd_res.stdout.replace(nl, '')}. It looks like you don't have FastInputIME installed :S"
            )
        else:
            logger.info("FastInputIME is the default keyboard.")
    else:
        logger.info("FastInputIME is the default keyboard.")
    if configs.args.screen_record:
        try:
            device.start_screenrecord()
        except Exception as e:
            logger.error(
                f"You can't use this feature without installing dependencies. Type that in console: 'pip3 install -U \"uiautomator2[image]\" -i https://pypi.doubanio.com/simple'. Exception: {e}"
            )
    return True


def close_instagram(device):
    logger.info("Close Instagram app.")
    device.deviceV2.app_stop(app_id)
    random_sleep(5, 5, modulable=False)
    if configs.args.screen_record:
        try:
            device.stop_screenrecord(crash=False)
        except Exception as e:
            logger.error(
                f"You can't use this feature without installing dependencies. Type that in console: 'pip3 install -U \"uiautomator2[image]\" -i https://pypi.doubanio.com/simple'. Exception: {e}"
            )


def check_if_crash_popup_is_there(device) -> bool:
    obj = device.find(resourceId=ResourceID.CRASH_POPUP)
    if obj.exists():
        obj.click()
        return True
    return False


def show_ending_conditions():
    end_likes = configs.args.end_if_likes_limit_reached
    end_follows = configs.args.end_if_follows_limit_reached
    end_watches = configs.args.end_if_watches_limit_reached
    end_comments = configs.args.end_if_comments_limit_reached
    end_pm = configs.args.end_if_pm_limit_reached
    logger.info(
        "-" * 70,
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    logger.info(
        f"{'Session ending conditions:':<35} Value",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    logger.info(
        "-" * 70,
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    logger.info(
        f"{'Likes:':<35} {end_likes}",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN if end_likes else Fore.RED}"},
    )
    logger.info(
        f"{'Follows:':<35} {end_follows}",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN if end_follows else Fore.RED}"},
    )
    logger.info(
        f"{'Watches:':<35} {end_watches}",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN if end_watches else Fore.RED}"},
    )
    logger.info(
        f"{'Comments:':<35} {end_comments}",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN if end_comments else Fore.RED}"},
    )
    logger.info(
        f"{'PM:':<35} {end_pm}",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN if end_pm else Fore.RED}"},
    )
    logger.info(
        f"{'Total actions:':<35} True (not mutable)",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN}"},
    )
    logger.info(
        f"{'Total successfull actions:':<35} True (not mutable)",
        extra={"color": f"{Style.BRIGHT}{Fore.GREEN}"},
    )
    logger.info(
        "For more info -> https://github.com/GramAddict/docs/blob/main/configuration.md#ending-session-conditions",
        extra={"color": f"{Style.BRIGHT}{Fore.BLUE}"},
    )
    logger.info(
        "-" * 70,
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )


def countdown(seconds: int = 10, waiting_message: str = "") -> None:
    while seconds:
        print(waiting_message, f"{seconds:02d}", end="\r")
        time.sleep(1)
        seconds -= 1


def choose_cloned_app(device) -> None:
    """if dialog box is displayed choose for original or cloned app"""
    app_number = "2" if configs.args.use_cloned_app else "1"
    obj = device.find(resourceId=f"{ResourceID.MIUI_APP}{app_number}")
    if obj.exists(3):
        logger.debug(f"Cloned app menu exists. Pressing on app number {app_number}.")
        obj.click()


def pre_post_script(path: str, pre: bool = True):
    if path is not None:
        if os.path.isfile(path):
            logger.info(f"Running '{path}' as {'pre' if pre else 'post'} script.")
            try:
                p1 = subprocess.Popen(path)
                p1.wait()
            except Exception as ex:
                logger.error(f"This exception has occurred: {ex}")
        else:
            logger.error(
                f"File '{path}' not found. Check your spelling. (The start point for relative paths is this: '{os.getcwd()}')."
            )


def print_telegram_reports(
    conf, telegram_reports_at_end, followers_now, following_now, time_left=None
):
    if followers_now is not None and telegram_reports_at_end:
        conf.actions["telegram-reports"].run(
            conf, "telegram-reports", followers_now, following_now, time_left
        )


def kill_atx_agent(device):
    _restore_keyboard(device)
    logger.info("Kill atx agent.")
    cmd: str = (
        f"adb{'' if configs.device_id is None else f' -s {configs.device_id}'} shell pkill atx-agent"
    )
    subprocess.run(cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8")


def restart_atx_agent(device):
    kill_atx_agent(device)
    logger.info("Restarting atx agent.")
    cmd: str = (
        f"adb{'' if configs.device_id is None else f' -s {configs.device_id}'} shell /data/local/tmp/atx-agent server -d"
    )

    try:
        result = subprocess.run(
            cmd, stdout=PIPE, stderr=PIPE, shell=True, encoding="utf8", check=True
        )
        if result.returncode != 0:
            logger.error(f"Failed to restart atx-agent: {result.stderr}")
        else:
            logger.info("atx-agent restarted successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while restarting atx-agent: {e}")


def _restore_keyboard(device):
    logger.debug("Back to default keyboard!")
    device.deviceV2.set_fastinput_ime(False)


def random_sleep(inf=0.5, sup=3.0, modulable=True, log=True):
    MIN_INF = 0.3
    multiplier = float(args.speed_multiplier)
    delay = uniform(inf, sup) / (multiplier if modulable else 1.0)
    delay = max(delay, MIN_INF)
    if log:
        logger.debug(f"{str(delay)[:4]}s sleep")
    sleep(delay)


def save_crash(device):
    directory_name = f"{__version__}_" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    crash_path = os.path.join("crashes", directory_name)
    try:
        os.makedirs(crash_path, exist_ok=False)
    except OSError:
        logger.error(f"Directory {directory_name} already exists.")
        return
    screenshot_format = ".png"
    try:
        device.screenshot(os.path.join(crash_path, "screenshot" + screenshot_format))
    except RuntimeError:
        logger.error(f"Cannot save 'screenshot.{screenshot_format}'.")

    hierarchy_format = ".xml"
    try:
        device.dump_hierarchy(os.path.join(crash_path, "hierarchy" + hierarchy_format))
    except RuntimeError:
        logger.error(f"Cannot save 'hierarchy.{hierarchy_format}'.")
    if args.screen_record:
        try:
            device.stop_screenrecord(crash=True)
        except Exception as e:
            logger.error(
                f"You can't use this feature without installing dependencies. Type that in console: 'pip3 install -U \"uiautomator2[image]\" -i https://pypi.doubanio.com/simple'. Exception: {e}"
            )
        files = [f for f in os.listdir("./") if f.endswith(".mp4")]
        try:
            os.replace(files[-1], os.path.join(crash_path, "video.mp4"))
        except (FileNotFoundError, IndexError):
            logger.error("File *.mp4 not found!")
    g_log_file_name, g_logs_dir, _, _ = get_log_file_config()
    src_file = os.path.join(g_logs_dir, g_log_file_name)
    target_file = os.path.join(crash_path, "logs.txt")
    trim_txt(source=src_file, target=target_file)  # copy logs trimmed
    shutil.make_archive(crash_path, "zip", crash_path)
    shutil.rmtree(crash_path)
    logger.info(
        f"Crash saved as {crash_path}.zip",
        extra={"color": Fore.GREEN},
    )
    logger.info(
        "If you want to report this crash, please upload the dump file via a ticket in the #lobby channel on discord ",
        extra={"color": Fore.GREEN},
    )
    logger.info("https://discord.gg/66zWWCDM7x\n", extra={"color": Fore.GREEN})
    check_if_updated(crash=True)
    if args.screen_record:
        try:
            device.start_screenrecord()
        except Exception as e:
            logger.error(
                f"You can't use this feature without installing dependencies. Type that in console: 'pip3 install -U \"uiautomator2[image]\" -i https://pypi.doubanio.com/simple'. Exception: {e}"
            )


def trim_txt(source: str, target: str) -> None:
    with open(source, "r", encoding="utf-8") as f:
        lines = f.readlines()
    tail = next(
        (
            index
            for index, line in enumerate(lines[::-1])
            if line.find("Arguments used:") != -1
        ),
        250,
    )
    rem = lines[-tail:]
    with open(target, "w", encoding="utf-8") as f:
        f.writelines(rem)


def stop_bot(device, sessions, session_state, was_sleeping=False):
    close_instagram(device)
    if args.kill_atx_agent:
        kill_atx_agent(device)
    head_up_notifications(enabled=True)
    logger.info(
        f"-------- FINISH: {datetime.now().strftime('%H:%M:%S')} --------",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    if session_state is not None:
        print_full_report(sessions, configs.args.scrape_to_file)
        if not was_sleeping:
            sessions.persist(directory=session_state.my_username)
    ask_for_a_donation()
    sys.exit(2)


def can_repeat(current_session, max_sessions: int) -> bool:
    if max_sessions == -1:
        return True
    logger.info(
        f"You completed {current_session} session(s). {max_sessions-current_session} session(s) left.",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    if current_session < max_sessions:
        return True
    logger.info(
        "You reached the total-sessions limit! Finish.",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    return False


def get_value(
    count: str,
    name: Optional[str],
    default: Optional[Union[int, float]] = 0,
    its_time: bool = False,
) -> Optional[Union[int, float]]:
    def print_error() -> None:
        logger.error(
            f'Using default value instead of "{count}", because it must be '
            "either a number (e.g. 2) or a range (e.g. 2-4)."
        )

    if count is None:
        return None
    try:
        if "." in count:
            value = float(count)
        else:
            value = int(count)
    except ValueError:
        parts = count.split("-")
        if len(parts) == 2:
            if not its_time:
                value = randint(int(parts[0]), int(parts[1]))
            else:
                value = round(uniform(int(parts[0]), int(parts[1])), 2)
        else:
            value = default
            print_error()
    if name is not None:
        logger.info(name.format(value), extra={"color": Style.BRIGHT})
    return value


def validate_url(x) -> bool:
    try:
        result = urlparse(x)
        return all([result.scheme, result.netloc, result.path])
    except Exception as e:
        logger.error(f"Error validating URL {x}. Error: {e}")
        return False


def append_to_file(filename: str, username: str) -> None:
    try:
        if not filename.lower().endswith(".txt"):
            filename += ".txt"
        with open(filename, "a+", encoding="utf-8") as file:
            file.write(username + "\n")
    except Exception as e:
        logger.error(f"Failed to append {username} to: {filename}. Exception: {e}")


def sample_sources(sources, n_sources):
    from random import sample

    sources_limit_input = n_sources.split("-")
    if len(sources_limit_input) > 1:
        sources_limit = randint(
            int(sources_limit_input[0]), int(sources_limit_input[1])
        )
    else:
        sources_limit = int(sources_limit_input[0])
    if len(sources) < sources_limit:
        sources_limit = len(sources)
    if sources_limit == 0:
        truncaded = sources
        shuffle(truncaded)
    else:
        truncaded = sample(sources, sources_limit)
        logger.info(
            f"Source list truncated at {len(truncaded)} {'item' if len(truncaded)<=1 else 'items'}."
        )
    logger.info(
        f"In this session, {'that source' if len(truncaded)<=1 else 'these sources'} will be handled: {', '.join(emoji.emojize(str(x), use_aliases=True) for x in truncaded)}"
    )
    return truncaded


def random_choice(number: int) -> bool:
    """
    Generate a random int and compare with the argument passed
    :param int number: number passed
    :return: is argument greater or equal then a random generated number
    :rtype: bool
    """
    return number >= randint(1, 100)


def init_on_things(source, args, sessions, session_state):
    from functools import partial

    from GramAddict.core.interaction import _on_interaction

    on_interaction = partial(
        _on_interaction,
        likes_limit=args.current_likes_limit,
        source=source,
        interactions_limit=get_value(
            args.interactions_count, "Interactions count: {}", 70
        ),
        sessions=sessions,
        session_state=session_state,
        args=args,
    )

    if args.stories_count != "0":
        stories_percentage = get_value(
            args.stories_percentage, "Chance of watching stories: {}%", 40
        )
    else:
        stories_percentage = 0

    likes_percentage = get_value(args.likes_percentage, "Chance of liking: {}%", 100)
    follow_percentage = get_value(
        args.follow_percentage, "Chance of following: {}%", 40
    )
    comment_percentage = get_value(
        args.comment_percentage, "Chance of commenting: {}%", 0
    )
    interact_percentage = get_value(
        args.interact_percentage, "Chance of interacting: {}%", 40
    )
    pm_percentage = get_value(args.pm_percentage, "Chance of send PM: {}%", 0)

    return (
        on_interaction,
        stories_percentage,
        likes_percentage,
        follow_percentage,
        comment_percentage,
        pm_percentage,
        interact_percentage,
    )


def set_time_delta(args):
    args.time_delta_session = (
        get_value(args.time_delta, None, 0) * (1 if random.random() < 0.5 else -1) * 60
    ) + random.randint(0, 59)
    m, s = divmod(abs(args.time_delta_session), 60)
    h, m = divmod(m, 60)
    logger.info(
        f"Time delta has set to {'' if args.time_delta_session >0 else '-'}{h:02d}:{m:02d}:{s:02d}."
    )


def wait_for_next_session(time_left, session_state, sessions, device):
    hours, remainder = divmod(time_left.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if args.kill_atx_agent:
        kill_atx_agent(device)
    logger.info(
        f'Next session will start at: {(datetime.now()+ time_left).strftime("%H:%M:%S (%Y/%m/%d)")}.',
        extra={"color": f"{Fore.GREEN}"},
    )
    logger.info(
        f"Time left: {hours:02d}:{minutes:02d}:{seconds:02d}.",
        extra={"color": f"{Fore.GREEN}"},
    )
    try:
        sleep(time_left.total_seconds())
    except KeyboardInterrupt:
        stop_bot(device, sessions, session_state, was_sleeping=True)


def inspect_current_view(user_list) -> Tuple[int, int]:
    """
    return the number of users and each row height in the current view
    """
    user_list.wait()
    lst = [item.get_height() for item in user_list if item.wait()]
    if not lst:
        raise EmptyList
    row_height, n_users = Counter(lst).most_common()[0]
    logger.debug(f"There are {n_users} users fully visible in that view.")
    return row_height, n_users


class ActionBlockedError(Exception):
    pass


class EmptyList(Exception):
    pass


class Square:
    def __init__(self, x0, y0, x1, y1):
        self.delta = 7
        self.x0 = x0
        self.y0 = y0
        self.x1 = x1
        self.y1 = y1

    def point(self):
        """return safe point to click"""
        if (self.x1 - self.x0) <= (2 * self.delta) or (self.y1 - self.y0) <= (
            2 * self.delta
        ):
            return nan
        else:
            return [
                randint(self.x0 + self.delta, self.x1 - self.delta),
                randint(self.y0 + self.delta, self.y1 - self.delta),
            ]
