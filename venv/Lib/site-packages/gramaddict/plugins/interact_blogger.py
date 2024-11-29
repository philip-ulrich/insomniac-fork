import logging
from functools import partial
from random import seed

from colorama import Style

from GramAddict.core.decorators import run_safely
from GramAddict.core.handle_sources import handle_blogger, handle_blogger_from_file
from GramAddict.core.interaction import (
    interact_with_user,
    is_follow_limit_reached_for_source,
)
from GramAddict.core.plugin_loader import Plugin
from GramAddict.core.utils import get_value, init_on_things, sample_sources

logger = logging.getLogger(__name__)

# Script Initialization
seed()


class InteractBloggerPostLikers(Plugin):
    """Handles the functionality of interacting with a blogger"""

    def __init__(self):
        super().__init__()
        self.description = "Handles the functionality of interacting with a blogger"
        self.arguments = [
            {
                "arg": "--blogger",
                "nargs": "+",
                "help": "interact a specified blogger",
                "metavar": ("blogger1", "blogger2"),
                "default": None,
                "operation": True,
            },
            {
                "arg": "--interact-from-file",
                "nargs": "+",
                "help": "filenames of the list of users [*.txt]",
                "metavar": ("filename1.txt", "filename2.txt"),
                "default": None,
                "operation": True,
            },
            {
                "arg": "--unfollow-from-file",
                "nargs": "+",
                "help": "filenames of the list of users [*.txt]",
                "metavar": ("filename1.txt", "filename2.txt"),
                "default": None,
                "operation": True,
            },
        ]

    def run(self, device, configs, storage, sessions, profile_filter, plugin):
        class State:
            def __init__(self):
                pass

            is_job_completed = False

        self.device_id = configs.args.device
        self.sessions = sessions
        self.session_state = sessions[-1]
        self.args = configs.args
        self.current_mode = plugin

        # Handle sources
        if plugin == "interact-from-file":
            sources = [f for f in self.args.interact_from_file if f.strip()]
        elif plugin == "unfollow-from-file":
            sources = [f for f in self.args.unfollow_from_file if f.strip()]
        else:
            sources = [s for s in self.args.blogger if s.strip()]

        for source in sample_sources(sources, self.args.truncate_sources):
            (
                active_limits_reached,
                unfollow_limits_reached,
                actions_limit_reached,
            ) = self.session_state.check_limit(limit_type=self.session_state.Limit.ALL)
            if plugin == "unfollow-from-file":
                limit_reached = unfollow_limits_reached or actions_limit_reached
            else:
                limit_reached = active_limits_reached or actions_limit_reached

            self.state = State()
            logger.info(f"Handle {source}", extra={"color": f"{Style.BRIGHT}"})

            # Init common things
            (
                on_interaction,
                stories_percentage,
                likes_percentage,
                follow_percentage,
                comment_percentage,
                pm_percentage,
                _,
            ) = init_on_things(source, self.args, self.sessions, self.session_state)

            @run_safely(
                device=device,
                device_id=self.device_id,
                sessions=self.sessions,
                session_state=self.session_state,
                screen_record=self.args.screen_record,
                configs=configs,
            )
            def job():
                self.handle_blogger(
                    device,
                    source,
                    plugin,
                    storage,
                    profile_filter,
                    on_interaction,
                    stories_percentage,
                    likes_percentage,
                    follow_percentage,
                    comment_percentage,
                    pm_percentage,
                )
                self.state.is_job_completed = True

            def job_file():
                self.handle_blogger_from_file(
                    device,
                    source,
                    plugin,
                    storage,
                    profile_filter,
                    on_interaction,
                    stories_percentage,
                    likes_percentage,
                    follow_percentage,
                    comment_percentage,
                    pm_percentage,
                )
                self.state.is_job_completed = True

            while not self.state.is_job_completed and not limit_reached:
                if plugin == "blogger":
                    job()
                else:
                    job_file()

            if limit_reached:
                logger.info("Ending session.")
                self.session_state.check_limit(
                    limit_type=self.session_state.Limit.ALL, output=True
                )
                break

    def handle_blogger(
        self,
        device,
        username,
        current_job,
        storage,
        profile_filter,
        on_interaction,
        stories_percentage,
        likes_percentage,
        follow_percentage,
        comment_percentage,
        pm_percentage,
    ):
        interaction = partial(
            interact_with_user,
            my_username=self.session_state.my_username,
            likes_count=self.args.likes_count,
            likes_percentage=likes_percentage,
            stories_percentage=stories_percentage,
            follow_percentage=follow_percentage,
            comment_percentage=comment_percentage,
            pm_percentage=pm_percentage,
            profile_filter=profile_filter,
            args=self.args,
            session_state=self.session_state,
            scraping_file=self.args.scrape_to_file,
            current_mode=self.current_mode,
        )
        source_follow_limit = (
            get_value(self.args.follow_limit, None, 15)
            if self.args.follow_limit is not None
            else None
        )
        is_follow_limit_reached = partial(
            is_follow_limit_reached_for_source,
            session_state=self.session_state,
            follow_limit=source_follow_limit,
            source=username,
        )

        handle_blogger(
            self,
            device,
            self.session_state,
            username,
            current_job,
            storage,
            profile_filter,
            on_interaction,
            interaction,
            is_follow_limit_reached,
        )

    def handle_blogger_from_file(
        self,
        device,
        current_filename,
        current_job,
        storage,
        profile_filter,
        on_interaction,
        stories_percentage,
        likes_percentage,
        follow_percentage,
        comment_percentage,
        pm_percentage,
    ):
        interaction = partial(
            interact_with_user,
            my_username=self.session_state.my_username,
            likes_count=self.args.likes_count,
            likes_percentage=likes_percentage,
            stories_percentage=stories_percentage,
            follow_percentage=follow_percentage,
            comment_percentage=comment_percentage,
            pm_percentage=pm_percentage,
            profile_filter=profile_filter,
            args=self.args,
            session_state=self.session_state,
            scraping_file=self.args.scrape_to_file,
            current_mode=self.current_mode,
        )
        source_follow_limit = (
            get_value(self.args.follow_limit, None, 15)
            if self.args.follow_limit is not None
            else None
        )
        is_follow_limit_reached = partial(
            is_follow_limit_reached_for_source,
            session_state=self.session_state,
            follow_limit=source_follow_limit,
            source=current_filename,
        )

        handle_blogger_from_file(
            self,
            device,
            current_filename,
            current_job,
            storage,
            on_interaction,
            interaction,
            is_follow_limit_reached,
        )
