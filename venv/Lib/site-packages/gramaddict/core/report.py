import logging
from datetime import datetime, timedelta

from colorama import Fore, Style

logger = logging.getLogger(__name__)


def print_full_report(sessions, scrape_mode):
    if len(sessions) > 1:
        for index, session in enumerate(sessions):
            finish_time = session.finishTime or datetime.now()
            logger.info(
                "",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            logger.info(
                f"SESSION #{index + 1}",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            logger.info(
                f"Start time: {session.startTime.strftime('%H:%M:%S (%Y/%m/%d)')}",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            logger.info(
                f"Finish time: {finish_time.strftime('%H:%M:%S (%Y/%m/%d)')}",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            duration = finish_time - session.startTime
            logger.info(
                f"Duration: {str(duration).split('.')[0]}",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            logger.info(
                f"Total interactions: {_stringify_interactions(session.totalInteractions)}",
                extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
            )
            if scrape_mode is None:
                logger.info(
                    f"Successful interactions: {_stringify_interactions(session.successfulInteractions)}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total followed: {_stringify_interactions(session.totalFollowed)}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total likes: {session.totalLikes}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total comments: {session.totalComments}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total PM sent: {session.totalPm}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total watched: {session.totalWatched}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
                logger.info(
                    f"Total unfollowed: {session.totalUnfollowed}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )
            else:
                logger.info(
                    f"Total scraped: {_stringify_interactions(session.totalScraped)}",
                    extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
                )

    logger.info(
        "",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )
    logger.info(
        "TOTAL",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )

    completed_sessions = [session for session in sessions if session.is_finished()]
    logger.info(
        f"Completed sessions: {len(completed_sessions)}",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )

    duration = timedelta(0)
    for session in sessions:
        finish_time = session.finishTime or datetime.now()
        duration += finish_time - session.startTime
    logger.info(
        f"Total duration: {str(duration).split('.')[0]}",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )

    total_interactions = {}
    total_interactions_num = 0
    successful_interactions = {}
    total_successful_interactions_num = 0
    total_followed = {}
    total_followed_num = 0
    total_scraped_num = 0
    total_scraped = {}
    for session in sessions:
        for source, count in session.totalInteractions.items():
            if total_interactions.get(source) is None:
                total_interactions[source] = count
            else:
                total_interactions[source] += count
            total_interactions_num += count
        for source, count in session.successfulInteractions.items():
            if successful_interactions.get(source) is None:
                successful_interactions[source] = count
            else:
                successful_interactions[source] += count
            total_successful_interactions_num += count

        for source, count in session.totalFollowed.items():
            if total_followed.get(source) is None:
                total_followed[source] = count
            else:
                total_followed[source] += count
            total_followed_num += count

        for source, count in session.totalScraped.items():
            if total_scraped.get(source) is None:
                total_scraped[source] = count
            else:
                total_scraped[source] += count
            total_scraped_num += count
    if scrape_mode is None:
        logger.info(
            f"Total interactions: ({total_interactions_num}) {_stringify_interactions(total_interactions)}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        logger.info(
            f"Successful interactions: ({total_successful_interactions_num}) {_stringify_interactions(successful_interactions)}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        logger.info(
            f"Total followed: ({total_followed_num}) {_stringify_interactions(total_followed)}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        total_likes = sum(session.totalLikes for session in sessions)
        logger.info(
            f"Total likes: {total_likes}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        total_comments = sum(session.totalComments for session in sessions)
        logger.info(
            f"Total comments: {total_comments}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        total_pm = sum(session.totalPm for session in sessions)
        logger.info(
            f"Total PM sent: {total_pm}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        total_watched = sum(session.totalWatched for session in sessions)
        logger.info(
            f"Total watched: {total_watched}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
        total_unfollowed = sum(session.totalUnfollowed for session in sessions)
        logger.info(
            f"Total unfollowed: {total_unfollowed}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )
    else:
        logger.info(
            f"Total users scraped: ({total_scraped_num}) {_stringify_interactions(total_scraped)}",
            extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
        )


def print_short_report(source, session_state):
    total_likes = session_state.totalLikes
    total_comments = session_state.totalComments
    total_pm = session_state.totalPm
    total_watched = session_state.totalWatched
    total_followed = sum(session_state.totalFollowed.values())
    interactions = session_state.successfulInteractions.get(source, 0)
    logger.info(
        f"Session progress: {total_likes} likes, {total_watched} watched, {total_comments} commented, {total_pm} PM sent, {total_followed} followed, {interactions} successful interaction(s) for {source}.",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )


def print_scrape_report(source, session_state):
    total_scraped = session_state.totalScraped.get(source)
    logger.info(
        f"Session progress: {total_scraped} user(s) scraped for {source}.",
        extra={"color": f"{Style.BRIGHT}{Fore.YELLOW}"},
    )


def _stringify_interactions(interactions):
    if len(interactions) == 0:
        return "0"

    result = ""
    for source, count in interactions.items():
        result += str(count) + " for " + source + ", "
    result = result[:-2]
    return result
