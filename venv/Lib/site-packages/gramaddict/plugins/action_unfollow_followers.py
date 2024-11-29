import logging
from enum import Enum, unique

from colorama import Fore

from GramAddict.core.decorators import run_safely
from GramAddict.core.device_facade import DeviceFacade, Timeout
from GramAddict.core.plugin_loader import Plugin
from GramAddict.core.resources import ClassName
from GramAddict.core.resources import ResourceID as resources
from GramAddict.core.scroll_end_detector import ScrollEndDetector
from GramAddict.core.storage import FollowingStatus
from GramAddict.core.utils import (
    get_value,
    inspect_current_view,
    random_sleep,
    save_crash,
)
from GramAddict.core.views import (
    Direction,
    FollowingView,
    ProfileView,
    UniversalActions,
)

logger = logging.getLogger(__name__)

FOLLOWING_REGEX = "^Following|^Requested"
UNFOLLOW_REGEX = "^Unfollow"


class ActionUnfollowFollowers(Plugin):
    """Handles the functionality of unfollowing your followers"""

    def __init__(self):
        super().__init__()
        self.description = "Handles the functionality of unfollowing your followers"
        self.arguments = [
            {
                "arg": "--unfollow",
                "nargs": None,
                "help": "unfollow at most given number of users. Only users followed by this script will be unfollowed. The order is from oldest to newest followings. It can be a number (e.g. 10) or a range (e.g. 10-20)",
                "metavar": "10-20",
                "default": None,
                "operation": True,
            },
            {
                "arg": "--unfollow-non-followers",
                "nargs": None,
                "help": "unfollow at most given number of users, that don't follow you back. Only users followed by this script will be unfollowed. The order is from oldest to newest followings. It can be a number (e.g. 10) or a range (e.g. 10-20)",
                "metavar": "10-20",
                "default": None,
                "operation": True,
            },
            {
                "arg": "--unfollow-any-non-followers",
                "nargs": None,
                "help": "unfollow at most given number of users, that don't follow you back. The order is from oldest to newest followings. It can be a number (e.g. 10) or a range (e.g. 10-20)",
                "metavar": "10-20",
                "default": None,
                "operation": True,
            },
            {
                "arg": "--unfollow-any-followers",
                "nargs": None,
                "help": "unfollow at most given number of users, that follow you back. The order is from oldest to newest followings. It can be a number (e.g. 10) or a range (e.g. 10-20)",
                "metavar": "10-20",
                "default": None,
                "operation": True,
            },
            {
                "arg": "--unfollow-any",
                "nargs": None,
                "help": "unfollow at most given number of users. The order is from oldest to newest followings. It can be a number (e.g. 10) or a range (e.g. 10-20)",
                "metavar": "10-20",
                "default": None,
                "operation": True,
            },
            {
                "arg": "--min-following",
                "nargs": None,
                "help": "minimum amount of followings, after reaching this amount unfollow stops",
                "metavar": "100",
                "default": 0,
            },
            {
                "arg": "--sort-followers-newest-to-oldest",
                "help": "sort the followers from newest to oldest instead of vice-versa (default)",
                "action": "store_true",
            },
            {
                "arg": "--unfollow-delay",
                "nargs": None,
                "help": "unfollow users followed by the bot after x amount of days",
                "metavar": "3",
                "default": "0",
            },
        ]

    def run(self, device, configs, storage, sessions, profile_filter, plugin):
        class State:
            def __init__(self):
                pass

            unfollowed_count = 0
            is_job_completed = False

        self.args = configs.args
        self.device_id = configs.args.device
        self.state = State()
        self.session_state = sessions[-1]
        self.sessions = sessions
        self.unfollow_type = plugin
        self.ResourceID = resources(self.args.app_id)

        count_arg = get_value(
            getattr(self.args, self.unfollow_type.replace("-", "_")),
            "Unfollow count: {}",
            10,
        )

        count = min(
            count_arg,
            self.session_state.my_following_count - int(self.args.min_following),
        )
        if count < 1:
            logger.warning(
                f"Now you're following {self.session_state.my_following_count} accounts, {'less then' if count <0 else 'equal to'} min following allowed (you set min-following: {self.args.min_following}). No further unfollows are required. Finish."
            )
            return
        elif self.session_state.my_following_count < count_arg:
            logger.warning(
                f"You can't unfollow {count_arg} accounts, because you are following {self.session_state.my_following_count} accounts. For that reason only {count} unfollows can be performed."
            )
        elif count < count_arg:
            logger.warning(
                f"You can't unfollow {count_arg} accounts, because you set min-following to {self.args.min_following} and you have {self.session_state.my_following_count} followers. For that reason only {count} unfollows can be performed."
            )

        if self.unfollow_type == "unfollow":
            self.unfollow_type = UnfollowRestriction.FOLLOWED_BY_SCRIPT
        elif self.unfollow_type == "unfollow-non-followers":
            self.unfollow_type = UnfollowRestriction.FOLLOWED_BY_SCRIPT_NON_FOLLOWERS
        elif self.unfollow_type == "unfollow-any-non-followers":
            self.unfollow_type = UnfollowRestriction.ANY_NON_FOLLOWERS
        elif self.unfollow_type == "unfollow-any-followers":
            self.unfollow_type = UnfollowRestriction.ANY_FOLLOWERS
        else:
            self.unfollow_type = UnfollowRestriction.ANY

        @run_safely(
            device=device,
            device_id=self.device_id,
            sessions=self.sessions,
            session_state=self.session_state,
            screen_record=self.args.screen_record,
            configs=configs,
        )
        def job():
            self.unfollow(
                device,
                count - self.state.unfollowed_count,
                self.on_unfollow,
                storage,
                self.unfollow_type,
                self.session_state.my_username,
                plugin,
            )
            logger.info(
                f"Unfollowed {self.state.unfollowed_count}, finish.",
                extra={"color": f"{Fore.CYAN}"},
            )
            self.state.is_job_completed = True
            device.back()

        while not self.state.is_job_completed and (self.state.unfollowed_count < count):
            job()

    def unfollow(
        self,
        device,
        count,
        on_unfollow,
        storage,
        unfollow_restriction,
        my_username,
        job_name,
    ):
        skipped_list_limit = get_value(self.args.skipped_list_limit, None, 15)
        skipped_fling_limit = get_value(self.args.fling_when_skipped, None, 0)
        posts_end_detector = ScrollEndDetector(
            repeats_to_end=2,
            skipped_list_limit=skipped_list_limit,
            skipped_fling_limit=skipped_fling_limit,
        )
        ProfileView(device).navigateToFollowing()
        self.iterate_over_followings(
            device,
            count,
            on_unfollow,
            storage,
            unfollow_restriction,
            my_username,
            posts_end_detector,
            job_name,
        )

    def on_unfollow(self):
        self.state.unfollowed_count += 1
        self.session_state.totalUnfollowed += 1

    def sort_followings_by_date(self, device, newest_to_oldest=False) -> bool:
        sort_button = device.find(
            resourceId=self.ResourceID.SORTING_ENTRY_ROW_OPTION,
        )
        if not sort_button.exists(Timeout.MEDIUM):
            logger.error(
                "Cannot find button to sort followings. Continue without sorting."
            )
            return False
        sort_button.click()

        sort_options_recycler_view = device.find(
            resourceId=self.ResourceID.FOLLOW_LIST_SORTING_OPTIONS_RECYCLER_VIEW
        )
        if not sort_options_recycler_view.exists(Timeout.MEDIUM):
            logger.error(
                "Cannot find options to sort followings. Continue without sorting."
            )
            return False
        if newest_to_oldest:
            logger.info("Sort followings by date: from newest to oldest.")
            sort_options_recycler_view.child(textContains="Latest").click()
        else:
            logger.info("Sort followings by date: from oldest to newest.")
            sort_options_recycler_view.child(textContains="Earliest").click()
        return True

    def iterate_over_followings(
        self,
        device,
        count,
        on_unfollow,
        storage,
        unfollow_restriction,
        my_username,
        posts_end_detector,
        job_name,
    ):
        # Wait until list is rendered
        sorted = False
        for _ in range(2):
            user_lst = device.find(
                resourceId=self.ResourceID.FOLLOW_LIST_CONTAINER,
                className=ClassName.LINEAR_LAYOUT,
            )
            user_lst.wait(Timeout.LONG)

            sort_container_obj = device.find(
                resourceId=self.ResourceID.SORTING_ENTRY_ROW_OPTION
            )
            if sort_container_obj.exists() and not sorted:
                sorted = self.sort_followings_by_date(
                    device, self.args.sort_followers_newest_to_oldest
                )
                continue

            top_tab_obj = device.find(
                resourceId=self.ResourceID.UNIFIED_FOLLOW_LIST_TAB_LAYOUT
            )
            if sort_container_obj.exists(Timeout.SHORT) and top_tab_obj.exists(
                Timeout.SHORT
            ):
                sort_container_bounds = sort_container_obj.get_bounds()["top"]
                list_tab_bounds = top_tab_obj.get_bounds()["bottom"]
                delta = sort_container_bounds - list_tab_bounds
                UniversalActions(device)._swipe_points(
                    direction=Direction.DOWN,
                    start_point_y=sort_container_bounds,
                    delta_y=delta - 50,
                )
            else:
                UniversalActions(device)._swipe_points(
                    direction=Direction.DOWN, delta_y=380
                )

            if sort_container_obj.exists() and not sorted:
                self.sort_followings_by_date(
                    device, self.args.sort_followers_newest_to_oldest
                )
                sorted = True
        checked = {}
        unfollowed_count = 0
        total_unfollows_limit_reached = False
        posts_end_detector.notify_new_page()
        prev_screen_iterated_followings = []
        while True:
            screen_iterated_followings = []
            logger.info("Iterate over visible followings.")
            user_list = device.find(
                resourceIdMatches=self.ResourceID.USER_LIST_CONTAINER,
            )
            row_height, n_users = inspect_current_view(user_list)
            for item in user_list:
                cur_row_height = item.get_height()
                if cur_row_height < row_height:
                    continue
                user_info_view = item.child(index=1)
                user_name_view = user_info_view.child(index=0).child()
                if not user_name_view.exists():
                    logger.info(
                        "Next item not found: probably reached end of the screen.",
                        extra={"color": f"{Fore.GREEN}"},
                    )
                    break

                username = user_name_view.get_text()
                screen_iterated_followings.append(username)
                if username not in checked:
                    checked[username] = None

                    if storage.is_user_in_whitelist(username):
                        logger.info(f"@{username} is in whitelist. Skip.")
                        continue

                    if unfollow_restriction in [
                        UnfollowRestriction.FOLLOWED_BY_SCRIPT,
                        UnfollowRestriction.FOLLOWED_BY_SCRIPT_NON_FOLLOWERS,
                    ]:
                        following_status = storage.get_following_status(username)
                        _, last_interaction = storage.check_user_was_interacted(
                            username
                        )
                        if following_status == FollowingStatus.NOT_IN_LIST:
                            logger.info(
                                f"@{username} has not been followed by this bot. Skip."
                            )
                            continue
                        elif not storage.can_be_unfollowed(
                            last_interaction,
                            get_value(self.args.unfollow_delay, None, 0),
                        ):
                            logger.info(
                                f"@{username} has been followed less then {self.args.unfollow_delay} days ago. Skip."
                            )
                            continue
                        elif following_status == FollowingStatus.UNFOLLOWED:
                            logger.info(
                                f"You have already unfollowed @{username} on {last_interaction}. Probably you got a soft ban at some point. Try again... Following status: {following_status.name}."
                            )
                        elif following_status not in (
                            FollowingStatus.FOLLOWED,
                            FollowingStatus.REQUESTED,
                        ):
                            logger.info(
                                f"Skip @{username}. Following status: {following_status.name}."
                            )
                            continue

                    if unfollow_restriction in [
                        UnfollowRestriction.ANY,
                        UnfollowRestriction.ANY_NON_FOLLOWERS,
                    ]:
                        following_status = storage.get_following_status(username)
                        if following_status == FollowingStatus.UNFOLLOWED:
                            logger.info(
                                f"Skip @{username}. Following status: {following_status.name}."
                            )
                            continue
                    if unfollow_restriction in [
                        UnfollowRestriction.ANY,
                        UnfollowRestriction.FOLLOWED_BY_SCRIPT,
                    ]:
                        unfollowed = FollowingView(device).do_unfollow_from_list(
                            user_row=item, username=username
                        )
                    else:
                        unfollowed = self.do_unfollow(
                            device,
                            username,
                            my_username,
                            unfollow_restriction
                            in [
                                UnfollowRestriction.FOLLOWED_BY_SCRIPT_NON_FOLLOWERS,
                                UnfollowRestriction.ANY_NON_FOLLOWERS,
                                UnfollowRestriction.ANY_FOLLOWERS,
                            ],
                            job_name == "unfollow-any-followers",
                        )

                    if unfollowed:
                        storage.add_interacted_user(
                            username,
                            self.session_state.id,
                            unfollowed=True,
                            job_name=job_name,
                            target=None,
                        )
                        on_unfollow()
                        unfollowed_count += 1
                        total_unfollows_limit_reached = self.session_state.check_limit(
                            limit_type=self.session_state.Limit.UNFOLLOWS,
                            output=True,
                        )
                    if unfollowed_count >= count or total_unfollows_limit_reached:
                        return
                else:
                    logger.debug(f"Already checked {username}.")

            if screen_iterated_followings != prev_screen_iterated_followings:
                prev_screen_iterated_followings = screen_iterated_followings
                logger.info("Need to scroll now.", extra={"color": f"{Fore.GREEN}"})
                list_view = device.find(
                    resourceId=self.ResourceID.LIST,
                )
                list_view.scroll(Direction.DOWN)
            else:
                load_more_button = device.find(
                    resourceId=self.ResourceID.ROW_LOAD_MORE_BUTTON
                )
                if load_more_button.exists():
                    load_more_button.click()
                    random_sleep()
                    if load_more_button.exists():
                        logger.warning(
                            "Can't iterate over the list anymore, you may be soft-banned and cannot perform this action (refreshing follower list)."
                        )
                        return
                    list_view.scroll(Direction.DOWN)
                else:
                    logger.info(
                        "Reached the following list end, finish.",
                        extra={"color": f"{Fore.GREEN}"},
                    )
                    return

    def do_unfollow(
        self,
        device: DeviceFacade,
        username,
        my_username,
        check_if_is_follower,
        unfollow_followers=False,
    ):
        """
        :return: whether unfollow was successful
        """
        username_view = device.find(
            resourceId=self.ResourceID.FOLLOW_LIST_USERNAME,
            className=ClassName.TEXT_VIEW,
            text=username,
        )
        if not username_view.exists():
            logger.error(f"Cannot find @{username}, skip.")
            return False
        username_view.click_retry()

        is_following_you = self.check_is_follower(device, username, my_username)
        if is_following_you is not None:
            if check_if_is_follower and is_following_you:
                if not unfollow_followers:
                    logger.info(f"Skip @{username}. This user is following you.")
                    logger.info("Back to the followings list.")
                    device.back()
                    return False
                else:
                    logger.info(f"@{username} is following you, unfollow. 😈")
            unfollow_button = device.find(
                classNameMatches=ClassName.BUTTON_OR_TEXTVIEW_REGEX,
                clickable=True,
                textMatches=FOLLOWING_REGEX,
            )
            # I don't know/remember the origin of this, if someone does - let's document it
            attempts = 2
            for _ in range(attempts):
                if unfollow_button.exists():
                    break

                scrollable = device.find(classNameMatches=ClassName.VIEW_PAGER)
                if scrollable.exists():
                    scrollable.scroll(Direction.UP)
                unfollow_button = device.find(
                    classNameMatches=ClassName.BUTTON_OR_TEXTVIEW_REGEX,
                    clickable=True,
                    textMatches=FOLLOWING_REGEX,
                )

            if not unfollow_button.exists():
                logger.error("Cannot find Following button.")
                save_crash(device)
            logger.debug("Unfollow button click.")
            unfollow_button.click()
            logger.info(f"Unfollow @{username}.", extra={"color": f"{Fore.YELLOW}"})

            # Weirdly enough, this is a fix for after you unfollow someone that follows
            # you back - the next person you unfollow the button is missing on first find
            # additional find - finds it. :shrug:
            confirm_unfollow_button = None
            attempts = 2
            for _ in range(attempts):
                confirm_unfollow_button = device.find(
                    resourceId=self.ResourceID.FOLLOW_SHEET_UNFOLLOW_ROW
                )
                if confirm_unfollow_button.exists(Timeout.SHORT):
                    break

            if not confirm_unfollow_button or not confirm_unfollow_button.exists():
                logger.error("Cannot confirm unfollow.")
                save_crash(device)
                device.back()
                return False
            logger.debug("Confirm unfollow.")
            confirm_unfollow_button.click()

            random_sleep(0, 1, modulable=False)

            # Check if private account confirmation
            private_unfollow_button = device.find(
                classNameMatches=ClassName.BUTTON_OR_TEXTVIEW_REGEX,
                textMatches=UNFOLLOW_REGEX,
            )
            if private_unfollow_button.exists(Timeout.SHORT):
                logger.debug("Confirm unfollow private account.")
                private_unfollow_button.click()

            UniversalActions.detect_block(device)
        else:
            logger.info("Back to the followings list.")
            device.back()
            return False
        logger.info("Back to the followings list.")
        device.back()
        return True

    def check_is_follower(self, device, username, my_username):
        logger.info(
            f"Check if @{username} is following you.", extra={"color": f"{Fore.GREEN}"}
        )

        if not ProfileView(device).navigateToFollowing():
            logger.info("Can't load profile in time. Skip.")
            return None

        rows = device.find(
            resourceId=self.ResourceID.FOLLOW_LIST_USERNAME,
            className=ClassName.TEXT_VIEW,
        )
        if rows.exists(Timeout.LONG):
            my_username_view = device.find(
                resourceId=self.ResourceID.FOLLOW_LIST_USERNAME,
                className=ClassName.TEXT_VIEW,
                text=my_username,
            )
            result = my_username_view.exists()
            logger.info("Back to the profile.")
            device.back()
            return result
        else:
            logger.info("Can't load profile followers in time. Skip.")
            device.back()
            return None


@unique
class UnfollowRestriction(Enum):
    ANY = 0
    FOLLOWED_BY_SCRIPT = 1
    FOLLOWED_BY_SCRIPT_NON_FOLLOWERS = 2
    ANY_NON_FOLLOWERS = 3
    ANY_FOLLOWERS = 4
