import logging

from colorama import Fore

logger = logging.getLogger(__name__)


class ScrollEndDetector:
    # Specify how many times we'll have to iterate over same users to decide that it's the end of the list
    repeats_to_end = 0
    skipped_all = 0
    skipped_all_fling = 0
    pages = []

    def __init__(
        self, repeats_to_end=5, skipped_list_limit=999, skipped_fling_limit=999
    ):
        self.repeats_to_end = repeats_to_end
        self.skipped_list_limit = skipped_list_limit
        self.skipped_fling_limit = skipped_fling_limit

    def notify_new_page(self):
        self.pages.append([])

    def notify_username_iterated(self, username):
        last_page = self.pages[-1]
        last_page.append(username)

    def reset_skipped_all(self):
        self.skipped_all = 0

    def notify_skipped_all(self):
        self.skipped_all += 1
        self.skipped_all_fling += 1

    def is_skipped_limit_reached(self):
        if self.skipped_all >= self.skipped_list_limit:
            logger.info(
                f"Skipped all users in list {self.skipped_list_limit} times. Finish.",
                extra={"color": f"{Fore.BLUE}"},
            )
            return True

    def is_fling_limit_reached(self):
        if (
            self.skipped_all_fling >= self.skipped_fling_limit
            and self.skipped_fling_limit > 0
        ):
            self.skipped_all_fling = 0
            return True

    def is_the_end(self):
        if len(self.pages) < 2:
            return False

        is_the_end = True
        last_page = self.pages[-1]
        repeats = 1
        for i in range(2, min(self.repeats_to_end + 1, len(self.pages) + 1)):
            page = self.pages[-i]
            if page != last_page:
                is_the_end = False
                break
            repeats += 1

        if is_the_end:
            logger.info(
                f"Same users iterated {repeats} times. End of the list.",
                extra={"color": f"{Fore.BLUE}"},
            )
        elif repeats > 1:
            logger.info(
                f"Same users iterated {repeats} times. Continue.",
                extra={"color": f"{Fore.BLUE}"},
            )

        return is_the_end
