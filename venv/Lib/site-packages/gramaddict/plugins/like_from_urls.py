import logging
import os
from os import path
from random import shuffle

from atomicwrites import atomic_write

from GramAddict.core.decorators import run_safely
from GramAddict.core.interaction import _browse_carousel, register_like
from GramAddict.core.plugin_loader import Plugin
from GramAddict.core.utils import open_instagram_with_url, validate_url
from GramAddict.core.views import MediaType, OpenedPostView, Owner, PostsViewList

logger = logging.getLogger(__name__)


class LikeFromURLs(Plugin):
    """Likes a post from url. The urls are read from a plaintext file"""

    def __init__(self):
        super().__init__()
        self.description = (
            "Likes a post from url. The urls are read from a plaintext file"
        )
        self.arguments = [
            {
                "arg": "--posts-from-file",
                "help": "full path of plaintext file contains urls to likes",
                "nargs": "+",
                "default": None,
                "metavar": ("postlist1.txt", "postlist2.txt"),
                "operation": True,
            }
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

        file_list = [file for file in (self.args.posts_from_file)]
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
        opened_post_view = OpenedPostView(self.device)
        post_view_list = PostsViewList(self.device)
        filename: str = os.path.join(storage.account_path, current_file.split(" ")[0])
        if path.isfile(filename):
            with open(filename, "r", encoding="utf-8") as f:
                nonempty_lines = [line.strip("\n") for line in f if line != "\n"]
                logger.info(f"In this file there are {len(nonempty_lines)} entries.")
                f.seek(0)
                for line in f:
                    url = line.strip()
                    if (
                        validate_url(url)
                        and "instagram.com/p/" in url
                        and open_instagram_with_url(url)
                    ):
                        already_liked, _ = opened_post_view._is_post_liked()
                        if already_liked:
                            logger.info("Post already liked!")
                        else:
                            _, content_desc = post_view_list._get_media_container()

                            (
                                media_type,
                                obj_count,
                            ) = post_view_list.detect_media_type(content_desc)
                            if media_type in (
                                MediaType.REEL,
                                MediaType.IGTV,
                                MediaType.VIDEO,
                            ):
                                opened_post_view.start_video()
                                video_opened = opened_post_view.open_video()
                                if video_opened:
                                    opened_post_view.watch_media(media_type)
                                    like_succeed = opened_post_view.like_video()
                                    logger.debug("Closing video...")
                                    self.device.back()
                            elif media_type in (
                                MediaType.CAROUSEL,
                                MediaType.PHOTO,
                            ):
                                if media_type == MediaType.CAROUSEL:
                                    _browse_carousel(self.device, obj_count)
                                opened_post_view.watch_media(media_type)
                                like_succeed = opened_post_view.like_post()

                            username, _, _ = post_view_list._post_owner(
                                self.current_mode, Owner.GET_NAME
                            )
                            if like_succeed:
                                register_like(self.device, self.session_state)
                                logger.info(f"Like for: {url}, status: {like_succeed}")
                                storage.add_interacted_user(
                                    username, self.session_state.id, liked=1
                                )
                            else:
                                logger.info("Not able to like this post!")
                    logger.info("Going back..")
                    self.device.back()
                remaining = f.readlines()

            if self.args.delete_interacted_users:
                with atomic_write(filename, overwrite=True, encoding="utf-8") as f:
                    f.writelines(remaining)
        else:
            logger.warning(f"File {current_file} not found.")
            return
