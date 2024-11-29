import json
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime
from enum import Enum, auto
from time import sleep
from typing import Optional, Tuple

import emoji
import yaml
from colorama import Fore, Style
from langdetect import detect

from GramAddict.core.config import get_time_last_save
from GramAddict.core.device_facade import Timeout
from GramAddict.core.resources import ResourceID as resources
from GramAddict.core.utils import random_sleep
from GramAddict.core.views import FollowStatus, ProfileView

logger = logging.getLogger(__name__)

FIELD_SKIP_BUSINESS = "skip_business"
FIELD_SKIP_NON_BUSINESS = "skip_non_business"
FIELD_SKIP_FOLLOWING = "skip_following"
FIELD_SKIP_FOLLOWER = "skip_follower"
FIELD_SKIP_IF_LINK_IN_BIO = "skip_if_link_in_bio"
FIELD_SKIP_PRIVATE = "skip_if_private"
FIELD_SKIP_PUBLIC = "skip_if_public"
FIELD_MIN_FOLLOWERS = "min_followers"
FIELD_MAX_FOLLOWERS = "max_followers"
FIELD_MIN_FOLLOWINGS = "min_followings"
FIELD_MAX_FOLLOWINGS = "max_followings"
FIELD_MIN_POTENCY_RATIO = "min_potency_ratio"
FIELD_MAX_POTENCY_RATIO = "max_potency_ratio"
FIELD_FOLLOW_PRIVATE_OR_EMPTY = "follow_private_or_empty"
FIELD_PM_TO_PRIVATE_OR_EMPTY = "pm_to_private_or_empty"
FIELD_COMMENT_PHOTOS = "comment_photos"
FIELD_COMMENT_VIDEOS = "comment_videos"
FIELD_COMMENT_CAROUSELS = "comment_carousels"
FIELD_BLACKLIST_WORDS = "blacklist_words"
FIELD_MANDATORY_WORDS = "mandatory_words"
FIELD_SPECIFIC_ALPHABET = "specific_alphabet"
FIELD_BIO_LANGUAGE = "biography_language"
FIELD_BIO_BANNED_LANGUAGE = "biography_banned_language"
FIELD_MIN_POSTS = "min_posts"
FIELD_MIN_LIKERS = "min_likers"
FIELD_MAX_LIKERS = "max_likers"
FIELD_MUTUAL_FRIENDS = "mutual_friends"

IGNORE_CHARSETS = ["MATHEMATICAL"]


def load_config(config):
    global args
    global configs
    global ResourceID
    args = config.args
    configs = config
    ResourceID = resources(config.args.app_id)


class SkipReason(Enum):
    YOU_FOLLOW = auto()
    FOLLOW_YOU = auto()
    IS_PRIVATE = auto()
    IS_PUBLIC = auto()
    UNKNOWN_PRIVACY = auto()
    LT_FOLLOWERS = auto()
    GT_FOLLOWERS = auto()
    LT_FOLLOWINGS = auto()
    GT_FOLLOWINGS = auto()
    POTENCY_RATIO = auto()
    HAS_BUSINESS = auto()
    HAS_NON_BUSINESS = auto()
    NOT_ENOUGH_POSTS = auto()
    BLACKLISTED_WORD = auto()
    MISSING_MANDATORY_WORDS = auto()
    ALPHABET_NOT_MATCH = auto()
    ALPHABET_NAME_NOT_MATCH = auto()
    BIOGRAPHY_LANGUAGE_NOT_MATCH = auto()
    NOT_LOADED = auto()
    RESTRICTED = auto()
    HAS_LINK_IN_BIO = auto()
    LT_MUTUAL = auto()
    BIOGRAPHY_IS_EMPTY = auto()


class Profile(object):
    def __init__(
        self,
        mutual_friends,
        follow_button_text,
        is_restricted,
        is_private,
        has_business_category,
        posts_count,
        biography,
        link_in_bio,
        fullname,
    ):
        self.datetime = str(datetime.now())
        self.followers = 0
        self.followings = 0
        self.mutual_friends = mutual_friends
        self.follow_button_text = follow_button_text
        self.is_restricted = is_restricted
        self.is_private = is_private
        self.has_business_category = has_business_category
        self.posts_count = posts_count
        self.biography = biography
        self.link_in_bio = link_in_bio
        self.fullname = fullname
        self.potency_ratio = None

    def set_followers_and_following(
        self, followers: Optional[int], followings: Optional[int]
    ) -> None:
        self.followers = followers
        self.followings = followings
        if followers is not None or followings is not None:
            self.potency_ratio = (
                0 if self.followings == 0 else self.followers / self.followings
            )


class Filter:
    conditions = None

    def __init__(self, storage=None):
        filter_path = storage.filter_path
        if configs.args.disable_filters:
            logger.warning(
                "Filters are disabled! (The default values in the documentation have been chosen!)"
            )
        elif os.path.exists(filter_path) and filter_path.endswith(".yml"):
            logger.warning(get_time_last_save(filter_path))
            with open(filter_path, "r", encoding="utf-8") as stream:
                try:
                    self.conditions = yaml.safe_load(stream)
                except Exception as e:
                    logger.error(f"Error: {e}")

        elif os.path.exists(filter_path):
            with open(filter_path, "r", encoding="utf-8") as json_file:
                try:
                    self.conditions = json.load(json_file)
                    logger.warning(
                        "Using filter.json is deprecated from version 2.3.0 and will stop working very soon, use filters.yml instead!"
                    )
                    sleep(5)
                except Exception as e:
                    logger.error(
                        f"Please check {json_file.name}, it contains this error: {e}"
                    )
                    sys.exit(2)
        self.storage = storage
        if self.conditions is not None:
            logger.info("-" * 70, extra={"color": f"{Fore.YELLOW}{Style.BRIGHT}"})
            logger.info(
                f"{'Filters recap (no spell check!)':<35} Value",
                extra={"color": f"{Fore.YELLOW}{Style.BRIGHT}"},
            )
            logger.info("-" * 70, extra={"color": f"{Fore.YELLOW}{Style.BRIGHT}"})
            for k, v in self.conditions.items():
                if isinstance(v, bool):
                    logger.info(
                        f"{k:<35} {v}",
                        extra={"color": f"{Fore.GREEN if v else Fore.RED}"},
                    )
                else:
                    logger.info(f"{k:<35} {v}", extra={"color": f"{Fore.WHITE}"})
        else:
            if not args.disable_filters:
                logger.warning(
                    f"The filters file doesn't exists in your account folder (can't find {filter_path}). Download it from https://github.com/GramAddict/bot/blob/08e1d7aff39ec47543fa78aadd7a2f034b9ae34d/config-examples/filters.yml and place it in your account folder!"
                )

    def is_num_likers_in_range(self, likes_on_post: str) -> bool:
        if self.conditions is not None and likes_on_post is not None:
            if likes_on_post == -1:
                logger.debug("We don't know how many likers this post has.")
                return True
            else:
                field_min_likers = self.conditions.get(FIELD_MIN_LIKERS, 1)
                field_max_likers = self.conditions.get(FIELD_MAX_LIKERS, 1000000)
                if likes_on_post in range(field_min_likers, field_max_likers):
                    logger.info(
                        f"Post has likes in range: {field_min_likers}-{field_max_likers}."
                    )
                    return True
                else:
                    logger.info(
                        f"Post has not likes in range: {field_min_likers}-{field_max_likers}."
                    )
                    return False
        else:
            logger.debug("filters.yml not loaded!")
            return True

    def return_check_profile(self, username, profile_data, skip_reason=None) -> bool:
        if self.storage is not None:
            self.storage.add_filter_user(username, profile_data, skip_reason)

        return skip_reason is not None

    def check_profile(self, device, username):
        """
        This method assumes being on someone's profile already.
        """
        if self.conditions is not None:
            field_skip_business = self.conditions.get(FIELD_SKIP_BUSINESS, False)
            field_skip_non_business = self.conditions.get(
                FIELD_SKIP_NON_BUSINESS, False
            )
            field_skip_following = self.conditions.get(FIELD_SKIP_FOLLOWING, False)
            field_skip_follower = self.conditions.get(FIELD_SKIP_FOLLOWER, False)
            field_min_followers = self.conditions.get(FIELD_MIN_FOLLOWERS)
            field_max_followers = self.conditions.get(FIELD_MAX_FOLLOWERS)
            field_min_followings = self.conditions.get(FIELD_MIN_FOLLOWINGS)
            field_max_followings = self.conditions.get(FIELD_MAX_FOLLOWINGS)
            field_min_potency_ratio = self.conditions.get(FIELD_MIN_POTENCY_RATIO, 0)
            field_max_potency_ratio = self.conditions.get(FIELD_MAX_POTENCY_RATIO, 999)
            field_blacklist_words = self.conditions.get(FIELD_BLACKLIST_WORDS, [])
            field_mandatory_words = self.conditions.get(FIELD_MANDATORY_WORDS, [])
            field_specific_alphabet = self.conditions.get(FIELD_SPECIFIC_ALPHABET)
            field_bio_language = self.conditions.get(FIELD_BIO_LANGUAGE)
            field_bio_banned_language = self.conditions.get(FIELD_BIO_BANNED_LANGUAGE)
            field_min_posts = self.conditions.get(FIELD_MIN_POSTS)
            field_mutual_friends = self.conditions.get(FIELD_MUTUAL_FRIENDS, -1)
            field_skip_if_link_in_bio = self.conditions.get(
                FIELD_SKIP_IF_LINK_IN_BIO, False
            )
            field_skip_if_private = self.conditions.get(FIELD_SKIP_PRIVATE, False)
            field_skip_if_public = self.conditions.get(FIELD_SKIP_PUBLIC, False)

        profile_data = self.get_all_data(device)
        if profile_data.is_restricted:
            logger.info(
                "This is a restricted profile, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.RESTRICTED
            )
        if profile_data.follow_button_text == FollowStatus.NONE or None in (
            profile_data.followers,
            profile_data.followings,
            profile_data.posts_count,
        ):
            logger.info(
                "Profile was not fully loaded, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.NOT_LOADED
            )
        if self.conditions is None:
            logger.debug("filters.yml not loaded!")
            return profile_data, False
        if (
            field_skip_following
            and profile_data.follow_button_text == FollowStatus.FOLLOWING
        ):
            logger.info(
                f"You follow @{username}, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.YOU_FOLLOW
            )

        if (
            field_skip_follower
            and profile_data.follow_button_text == FollowStatus.FOLLOW_BACK
        ):
            logger.info(
                f"@{username} follows you, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.FOLLOW_YOU
            )
        logger.debug(
            f"This account is {'private' if profile_data.is_private else 'public'}."
        )

        if profile_data.is_private and field_skip_if_public:
            logger.info(
                f"@{username} has public account and you want to interact only private, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.IS_PUBLIC
            )
        elif profile_data.is_private and field_skip_if_private:
            logger.info(
                f"@{username} has private account and you want to interact only public, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.IS_PRIVATE
            )
        elif profile_data.is_private is None:
            logger.info(
                f"Could not determine if @{username} is public or private, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.UNKNOWN_PRIVACY
            )

        logger.debug("Checking if account is within follower/following parameters...")
        if field_min_followers is not None and profile_data.followers < int(
            field_min_followers
        ):
            logger.info(
                f"@{username} has less than {field_min_followers} followers, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.LT_FOLLOWERS
            )
        if field_max_followers is not None and profile_data.followers > int(
            field_max_followers
        ):
            logger.info(
                f"@{username} has more than {field_max_followers} followers, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.GT_FOLLOWERS
            )
        if field_min_followings is not None and profile_data.followings < int(
            field_min_followings
        ):
            logger.info(
                f"@{username} has less than {field_min_followings} followings, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.LT_FOLLOWINGS
            )
        if field_max_followings is not None and profile_data.followings > int(
            field_max_followings
        ):
            logger.info(
                f"@{username} has more than {field_max_followings} followings, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.GT_FOLLOWINGS
            )

        if (field_min_potency_ratio != 0 or field_max_potency_ratio != 999) and (
            (
                int(profile_data.followings) == 0
                or profile_data.followers / profile_data.followings
                < float(field_min_potency_ratio)
                or profile_data.followers / profile_data.followings
                > float(field_max_potency_ratio)
            )
        ):
            logger.info(
                f"@{username}'s potency ratio is not between {field_min_potency_ratio} and {field_max_potency_ratio}, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.POTENCY_RATIO
            )

        if field_mutual_friends != -1:
            logger.debug(
                f"Checking if that user has at least {field_mutual_friends} mutual friends."
            )
            if profile_data.mutual_friends < field_mutual_friends:
                logger.info(
                    f"@{username} has less then {field_mutual_friends} mutual friends, skip.",
                    extra={"color": f"{Fore.CYAN}"},
                )
                return profile_data, self.return_check_profile(
                    username, profile_data, SkipReason.LT_MUTUAL
                )

        if field_skip_if_link_in_bio:
            logger.debug("Checking if account has link in bio...")
            if profile_data.link_in_bio is not None:
                logger.info(
                    f"@{username} has a link in bio, skip.",
                    extra={"color": f"{Fore.CYAN}"},
                )
                return profile_data, self.return_check_profile(
                    username, profile_data, SkipReason.HAS_LINK_IN_BIO
                )

        if field_skip_business or field_skip_non_business:
            logger.debug("Checking if account is a business...")
        if field_skip_business and profile_data.has_business_category is True:
            logger.info(
                f"@{username} has business account, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.HAS_BUSINESS
            )
        if field_skip_non_business and profile_data.has_business_category is False:
            logger.info(
                f"@{username} has non business account, skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.HAS_NON_BUSINESS
            )

        if field_min_posts is not None and field_min_posts > profile_data.posts_count:
            logger.info(
                f"@{username} doesn't have enough posts ({profile_data.posts_count}), skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.NOT_ENOUGH_POSTS
            )

        cleaned_biography = " ".join(
            emoji.get_emoji_regexp()
            .sub("", profile_data.biography.replace("\n", ""))
            .lower()
            .split()
        )

        if not cleaned_biography and (
            len(field_mandatory_words) > 0
            or field_bio_language is not None
            or field_specific_alphabet is not None
        ):
            logger.info(
                f"@{username} has an empty biography, that means there isn't any mandatory things that can be checked. Skip.",
                extra={"color": f"{Fore.CYAN}"},
            )
            return profile_data, self.return_check_profile(
                username, profile_data, SkipReason.BIOGRAPHY_IS_EMPTY
            )
        if (
            len(field_blacklist_words) > 0
            or len(field_mandatory_words) > 0
            or field_specific_alphabet is not None
            or field_bio_language is not None
            or field_bio_banned_language is not None
        ):
            logger.debug("Pulling biography...")
            if len(field_blacklist_words) > 0:
                logger.debug(
                    "Checking if account has blacklisted words in biography..."
                )
                # If we found a blacklist word return False
                for w in field_blacklist_words:
                    blacklist_words = re.compile(
                        r"\b({0})\b".format(w), flags=re.IGNORECASE
                    ).search(cleaned_biography)
                    if blacklist_words is not None:
                        logger.info(
                            f"@{username} found a blacklisted word '{w}' in biography, skip.",
                            extra={"color": f"{Fore.CYAN}"},
                        )
                        return profile_data, self.return_check_profile(
                            username, profile_data, SkipReason.BLACKLISTED_WORD
                        )

            if len(field_mandatory_words) > 0:
                logger.debug("Checking if account has mandatory words in biography...")
                mandatory_words = [
                    w
                    for w in field_mandatory_words
                    if re.compile(r"\b({0})\b".format(w), flags=re.IGNORECASE).search(
                        cleaned_biography
                    )
                    is not None
                ]
                if mandatory_words == []:
                    logger.info(
                        f"@{username} mandatory words not found in biography, skip.",
                        extra={"color": f"{Fore.CYAN}"},
                    )
                    return profile_data, self.return_check_profile(
                        username, profile_data, SkipReason.MISSING_MANDATORY_WORDS
                    )

            if field_specific_alphabet is not None:
                logger.debug("Checking primary character set of account biography...")
                alphabet = self._find_alphabet(cleaned_biography)

                if alphabet not in field_specific_alphabet and alphabet != "":
                    logger.info(
                        f"@{username}'s biography alphabet is not in {', '.join(field_specific_alphabet)}. ({alphabet}), skip.",
                        extra={"color": f"{Fore.CYAN}"},
                    )
                    return profile_data, self.return_check_profile(
                        username, profile_data, SkipReason.ALPHABET_NOT_MATCH
                    )
            if field_bio_language is not None or field_bio_banned_language is not None:
                skip_1 = skip_2 = False
                logger.debug("Checking main language of account biography...")
                language = self._find_language(cleaned_biography)
                if (
                    field_bio_banned_language
                    and language in field_bio_banned_language
                    and language != ""
                ):
                    logger.info(
                        f"@{username}'s biography language is in the banned list: {', '.join(field_bio_banned_language)}. ({language}), skip.",
                        extra={"color": f"{Fore.CYAN}"},
                    )
                    skip_1 = True
                if (
                    not skip_1
                    and field_bio_language
                    and language not in field_bio_language
                    and language != ""
                ):
                    logger.info(
                        f"@{username}'s biography language is not in the list: {', '.join(field_bio_language)}. ({language}), skip.",
                        extra={"color": f"{Fore.CYAN}"},
                    )
                    skip_2 = True
                if skip_1 or skip_2:
                    return profile_data, self.return_check_profile(
                        username,
                        profile_data,
                        SkipReason.BIOGRAPHY_LANGUAGE_NOT_MATCH,
                    )

        if field_specific_alphabet is not None:
            logger.debug("Checking primary character set of name...")
            if profile_data.fullname != "":
                alphabet = self._find_alphabet(profile_data.fullname)
                if alphabet not in field_specific_alphabet and alphabet != "":
                    logger.info(
                        f"@{username}'s name alphabet is not in {', '.join(field_specific_alphabet)}. ({alphabet}), skip.",
                        extra={"color": f"{Fore.CYAN}"},
                    )
                    return profile_data, self.return_check_profile(
                        username,
                        profile_data,
                        SkipReason.ALPHABET_NAME_NOT_MATCH,
                    )

        # If no filters return false, we are good to proceed
        return profile_data, self.return_check_profile(username, profile_data, None)

    def can_follow_private_or_empty(self) -> bool:
        if self.conditions is None:
            return False

        field_follow_private_or_empty = self.conditions.get(
            FIELD_FOLLOW_PRIVATE_OR_EMPTY
        )
        return field_follow_private_or_empty is not None and bool(
            field_follow_private_or_empty
        )

    def can_pm_to_private_or_empty(self) -> bool:
        if self.conditions is None:
            return False

        field_pm_to_private_or_empty = self.conditions.get(FIELD_PM_TO_PRIVATE_OR_EMPTY)
        return field_pm_to_private_or_empty is not None and bool(
            field_pm_to_private_or_empty
        )

    def can_comment(self, current_mode) -> Tuple[bool, bool, bool, bool]:
        if self.conditions is not None:
            return (
                self.conditions.get(FIELD_COMMENT_PHOTOS, True),
                self.conditions.get(FIELD_COMMENT_VIDEOS, True),
                self.conditions.get(FIELD_COMMENT_CAROUSELS, True),
                self.conditions.get("comment_" + current_mode.replace("-", "_"), False),
            )
        else:
            logger.debug("filters.yml (or legacy filter.json) is not loaded!")
        return False, False, False, False

    def get_all_data(self, device):
        profile_picture = device.find(
            resourceIdMatches=ResourceID.PROFILE_HEADER_AVATAR_CONTAINER_TOP_LEFT_STUB
        )
        restricted_profile = device.find(
            resourceIdMatches=ResourceID.RESTRICTED_ACCOUNT_TITLE
        )
        is_restricted = False
        if not profile_picture.exists(Timeout.LONG):
            if restricted_profile.exists():
                is_restricted = True
            else:
                logger.warning(
                    "Looks like this profile hasn't loaded yet! Wait a little bit more.."
                )
                if profile_picture.exists(Timeout.LONG):
                    logger.info("Profile loaded!")
                else:
                    logger.warning(
                        "Profile not fully loaded after 16s. Is your connection ok? Let's sleep for 1-2 minutes."
                    )
                    random_sleep(60, 120, modulable=False)
                    if profile_picture.exists():
                        logger.warning(
                            "Profile won't load! Maybe you're soft-banned or you've lost your connection!"
                        )
        profileView = ProfileView(device)
        if not is_restricted:
            profile = Profile(
                mutual_friends=self._get_mutual_friends(device, profileView),
                follow_button_text=self._get_follow_button_text(device, profileView),
                is_restricted=is_restricted,
                is_private=self._is_private_account(device, profileView),
                has_business_category=self._has_business_category(device, profileView),
                posts_count=self._get_posts_count(device, profileView),
                biography=self._get_profile_biography(device, profileView),
                link_in_bio=self._get_link_in_bio(device, profileView),
                fullname=self._get_fullname(device, profileView),
            )
            followers, following = self._get_followers_and_followings(device)
            profile.set_followers_and_following(followers, following)
        else:
            profile = Profile(
                mutual_friends=None,
                follow_button_text=None,
                is_restricted=is_restricted,
                is_private=None,
                has_business_category=None,
                posts_count=None,
                biography=None,
                link_in_bio=None,
                fullname=None,
            )
            profile.set_followers_and_following(None, None)
        return profile

    @staticmethod
    def _get_followers_and_followings(
        device, profileView: ProfileView = None
    ) -> Tuple[int, int]:
        followers = 0
        profileView = ProfileView(device) if profileView is None else profileView
        try:
            followers = profileView.getFollowersCount()
        except Exception as e:
            logger.error(f"Cannot find followers count view, default is {followers}.")
            logger.debug(f"Error: {e}")

        followings = 0
        try:
            followings = profileView.getFollowingCount()
        except Exception as e:
            logger.error(f"Cannot find followings count view, default is {followings}.")
            logger.debug(f"Error: {e}")
        if followers is not None and followings is not None:
            return followers, followings
        else:
            return 0, 1

    @staticmethod
    def _has_business_category(device, ProfileView=None) -> bool:
        business_category_view = device.find(
            resourceId=ResourceID.PROFILE_HEADER_BUSINESS_CATEGORY,
        )
        return business_category_view.exists()

    @staticmethod
    def _is_private_account(device, profileView: ProfileView = None) -> Optional[bool]:
        private = None
        profileView = ProfileView(device) if profileView is None else profileView
        try:
            private = profileView.isPrivateAccount()
        except Exception as e:
            logger.error("Cannot find whether it is private or not")
            logger.debug(f"Error: {e}")

        return private

    @staticmethod
    def _get_profile_biography(device, profileView: ProfileView = None) -> str:
        profileView = ProfileView(device) if profileView is None else profileView
        return profileView.getProfileBiography()

    @staticmethod
    def _find_alphabet(biography: str) -> str:
        a_dict = {}
        max_alph = "UNKNOWN"
        try:
            for x in range(len(biography)):
                if biography[x].isalpha():
                    a = unicodedata.name(biography[x]).split(" ")[0]
                    if a not in IGNORE_CHARSETS:
                        if a in a_dict:
                            a_dict[a] += 1
                        else:
                            a_dict[a] = 1
            if bool(a_dict):
                max_alph = max(a_dict, key=lambda k: a_dict[k])
        except Exception as e:
            logger.error(f"Cannot determine primary alphabet. Error: {e}")

        return max_alph

    @staticmethod
    def _find_language(biography: str) -> str:
        """Language detection algorithm is non-deterministic, which means that if you try to run it on a text which is either too short or too ambiguous, you might get different results everytime you run it."""
        language = ""
        results = []
        try:
            for _ in range(5):
                # we do a BO5, that would mitigate the inconsistency a little
                results.append(detect(biography))
            language = max(results, key=results.count)
        except Exception as e:
            logger.error(f"Cannot determine primary language. Error: {e}")
        return language

    @staticmethod
    def _get_fullname(device, profileView: ProfileView = None) -> str:
        profileView = ProfileView(device) if profileView is None else profileView
        fullname = ""
        try:
            fullname = profileView.getFullName()
        except Exception as e:
            logger.error("Cannot find full name.")
            logger.debug(f"Error: {e}")

        return fullname

    @staticmethod
    def _get_posts_count(device, profileView: ProfileView = None) -> int:
        profileView = ProfileView(device) if profileView is None else profileView
        posts_count = 0
        try:
            posts_count = profileView.getPostsCount()
        except Exception as e:
            logger.error("Cannot find posts count. Default is 0.")
            logger.debug(f"Error: {e}")

        return posts_count

    @staticmethod
    def _get_follow_button_text(device, profileView: ProfileView = None) -> str:
        profileView = ProfileView(device) if profileView is None else profileView
        _, text = profileView.getFollowButton()
        return text

    @staticmethod
    def _get_mutual_friends(device, profileView: ProfileView = None) -> int:
        profileView = ProfileView(device) if profileView is None else profileView
        return profileView.getMutualFriends()

    @staticmethod
    def _get_link_in_bio(device, profileView: ProfileView = None) -> str:
        profileView = ProfileView(device) if profileView is None else profileView
        return profileView.getLinkInBio()
