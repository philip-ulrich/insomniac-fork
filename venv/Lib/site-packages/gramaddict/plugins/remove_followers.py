import logging
import os
from os import path
from random import shuffle

from atomicwrites import atomic_write
from colorama import Fore

from GramAddict.core.decorators import run_safely
from GramAddict.core.plugin_loader import Plugin
from GramAddict.core.utils import get_value
from GramAddict.core.views import FollowersView, ProfileView, UniversalActions

logger = logging.getLogger(__name__)


class RemoveFollowersFromList(Plugin):
    """Remove account followers from a list of usernames"""

    def __init__(self):
        super().__init__()
        self.description = "Remove followers from a list"
        self.arguments = [
            {
                "arg": "--remove-followers-from-file",
                "help": "full path of plaintext file contains followers to be removed",
                "nargs": "+",
                "default": None,
                "metavar": ("remove1.txt", "remove2.txt"),
                "operation": True,
            },
            {
                "arg": "--delete-removed-followers",
                "help": "delete the followers removed from the txt",
                "action": "store_true",
            },
        ]

    def run(self, device, configs, storage, sessions, profile_filter, plugin):
        class State:
            def __init__(self):
                pass

            is_job_completed = False

        self.args = configs.args
        self.device = device
        self.device_id = configs.args.device
        self.state = None
        self.sessions = sessions
        self.session_state = sessions[-1]
        self.current_mode = plugin

        file_list = [file for file in self.args.remove_followers_from_file]
        shuffle(file_list)

        for filename in file_list:
            self.state = State()

            @run_safely(
                device=self.device,
                device_id=self.device_id,
                sessions=self.sessions,
                session_state=self.session_state,
                screen_record=self.args.screen_record,
                configs=configs,
            )
            def job():
                self.process_file(filename, storage)

            job()

    def process_file(self, current_file, storage):
        followers_view = FollowersView(self.device)
        universal_actions = UniversalActions(self.device)
        filename: str = os.path.join(storage.account_path, current_file.split(" ")[0])
        try:
            amount_of_users = get_value(current_file.split(" ")[1], None, 10)
        except IndexError:
            amount_of_users = 10
            logger.warning(
                f"You didn't passed how many users should be processed from the list! Default is {amount_of_users} users."
            )
        if path.isfile(filename):
            with open(filename, "r", encoding="utf-8") as f:
                nonempty_lines = [line.strip("\n") for line in f if line != "\n"]
                logger.info(
                    f"In {filename} there are {len(nonempty_lines)} entries.",
                    extra={"color": f"{Fore.GREEN}"},
                )
                f.seek(0)
                ProfileView(self.device).navigateToFollowers()
                users_removed = 0
                for line in f:
                    username = line.strip()
                    universal_actions.search_text(username)
                    universal_actions.close_keyboard(self.device)
                    if followers_view.remove_follower(username):
                        logger.info(
                            f"{username} has been removed from your followers list.",
                            extra={"color": f"{Fore.GREEN}"},
                        )
                        users_removed += 1
                    else:
                        logger.info(
                            f"{username} has not been removed from your followers list.",
                            extra={"color": f"{Fore.RED}"},
                        )
                    if users_removed == amount_of_users:
                        logger.info(
                            f"{users_removed} followers have been removed.",
                            extra={"color": f"{Fore.BLUE}"},
                        )
                        break
                remaining = f.readlines()

            if self.args.delete_removed_followers:
                with atomic_write(filename, overwrite=True, encoding="utf-8") as f:
                    f.writelines(remaining)
        else:
            logger.warning(f"File {filename} not found.")
            return
