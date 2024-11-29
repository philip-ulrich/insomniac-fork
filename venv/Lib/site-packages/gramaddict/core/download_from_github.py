### This module comes out of https://github.com/sdushantha/gitdir
### MIT License
### Copyright (c) 2019 Siddharth Dushantha

import json
import os
import random
import re
import sys
import urllib.request
from pathlib import Path

from colorama import Fore, Style

# this ANSI code lets us erase the current line
ERASE_LINE = "\x1b[2K"
COLOR_NAME_TO_CODE = {
    "default": "",
    "red": Fore.RED,
    "green": Style.BRIGHT + Fore.GREEN,
}


def print_text(
    text, color="default", in_place=False, **kwargs
):  # type: (str, str, bool, any) -> None
    """
    print text to console, a wrapper to built-in print

    :param text: text to print
    :param color: can be one of "red" or "green", or "default"
    :param in_place: whether to erase previous line and print in place
    :param kwargs: other keywords passed to built-in print
    """
    if in_place:
        print("\r" + ERASE_LINE, end="")
    print(COLOR_NAME_TO_CODE[color] + text + Style.RESET_ALL, **kwargs)


def get_proxy(proxies=None):
    proxy = urllib.request.ProxyHandler({})
    if proxies is not None:
        option = "http://" + random.choice(proxies)
        print_text(f"\nTrying Proxy: {option}", "green", in_place=True)
        proxy = urllib.request.ProxyHandler({"http": option})
    return proxy


def create_url(url):
    """
    From the given url, produce a URL that is compatible with Github's REST API. Can handle blob or tree paths.
    """
    repo_only_url = re.compile(
        r"https:\/\/github\.com\/[a-z\d](?:[a-z\d]|-(?=[a-z\d])){0,38}\/[a-zA-Z0-9]+$"
    )
    re_branch = re.compile("/(tree|blob)/(.+?)/")

    # Check if the given url is a url to a GitHub repo. If it is, tell the
    # user to use 'git clone' to download it
    branch = re_branch.search(url)
    if re.match(repo_only_url, url) and branch is None:
        print_text(
            "✘ The given url is a complete repository. Use 'git clone' to download the repository",
            "red",
            in_place=True,
        )
        sys.exit()

    # extract the branch name from the given url (e.g master)
    if branch:
        download_dirs = url[branch.end() :]
        api_url = (
            url[: branch.start()].replace("github.com", "api.github.com/repos", 1)
            + "/contents/"
            + download_dirs
            + "?ref="
            + branch.group(2)
        )
        return api_url, download_dirs.split("/")[-1]
    else:
        print_text(
            "✘ Couldn't find the repo, Pls check the URL!!!", "red", in_place=True
        )
        sys.exit()


def download_from_github(
    repo_url, proxies=None, output_dir="./", flatten=True, exts=None, file_count=0
):
    """Downloads the files and directories in repo_url. If flatten is specified, the contents of any and all
    sub-directories will be pulled upwards into the root folder."""

    # handle paths cross platform
    output_dir = Path(output_dir)

    # getting proxy from proxy list
    proxy = get_proxy(proxies)

    # generate the url which returns the JSON data
    api_url, download_dir = create_url(repo_url)

    # To handle file names.
    if not flatten:
        dir_out = Path(output_dir) / download_dir
    else:
        dir_out = Path(output_dir)

        # trying to get api response
    try:
        opener = urllib.request.build_opener(proxy)
        opener.addheaders = [
            (
                "User-agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            )
        ]
        urllib.request.install_opener(opener)
        response = urllib.request.urlretrieve(api_url)
    except KeyboardInterrupt:
        # when CTRL+C is pressed during the execution of this script,
        # bring the cursor to the beginning, erase the current line, and dont make a new line
        print_text("✘ Got interrupted", "red", in_place=True)
        sys.exit()
    except urllib.error.HTTPError as e:
        if e.code == 403:  # Api response 403 error
            print_text("API Rate limit exceeded!!!", "red", in_place=True)
            # download_from_github(repo_url, proxies, dir_out, flatten, exts=exts, file_count=file_count)
        else:
            print_text(e, "red", in_place=True)
        sys.exit()
    except:
        print_text("✘ Failed", "red", in_place=True)
        sys.exit()

    # make a directory with the name which is taken from
    # the actual repo
    try:
        os.makedirs(dir_out)
    except FileExistsError:
        pass

    with open(response[0], "r") as f:
        data = json.load(f)
        # getting the total number of files so that we
        # can use it for the output information later

        # If the data is a file, download it as one.
        if isinstance(data, dict) and data["type"] == "file":
            try:
                # download the file
                opener = urllib.request.build_opener(proxy)
                opener.addheaders = [
                    (
                        "User-agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
                    )
                ]
                urllib.request.install_opener(opener)
                if exts is None:
                    urllib.request.urlretrieve(
                        data["download_url"], Path(dir_out) / data["name"]
                    )
                    file_count += 1
                    print_text(
                        "Downloaded: " + Fore.WHITE + "{}".format(data["name"]),
                        "green",
                        in_place=True,
                    )
                if (
                    exts is not None
                    and os.path.splitext(data["download_url"])[1] in exts
                ):
                    urllib.request.urlretrieve(
                        data["download_url"], Path(dir_out) / data["name"]
                    )
                    file_count += 1
                    print_text(
                        "Downloaded: " + Fore.WHITE + "{}".format(data["name"]),
                        "green",
                        in_place=True,
                    )
                return file_count
            except KeyboardInterrupt:
                # when CTRL+C is pressed during the execution of this script,
                # bring the cursor to the beginning, erase the current line, and dont make a new line
                print_text("✘ Got interrupted", "red", in_place=False)
                sys.exit()
            except urllib.error.HTTPError as e:
                if e.code == 403:
                    print_text("API Rate limit exceeded!!!", "red", in_place=True)
                    # download_from_github(data["html_url"], proxies, dir_out, flatten, exts=exts, file_count=file_count)
                else:
                    print_text(e, "red", in_place=True)
                sys.exit()
            except:
                print_text("✘ Failed", "red", in_place=True)
                sys.exit()

        # going over the files in the directory
        for file in data:
            file_url = file["download_url"]
            file_name = file["name"]

            if file_url is not None:
                try:
                    # if it's a file
                    path = Path(dir_out) / file_name
                    opener = urllib.request.build_opener(proxy)
                    opener.addheaders = [
                        (
                            "User-agent",
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
                        )
                    ]
                    urllib.request.install_opener(opener)
                    # download the file
                    if exts is None:
                        urllib.request.urlretrieve(file_url, path)
                        file_count += 1
                        print_text(
                            "Downloaded: " + Fore.WHITE + "{}".format(file_name),
                            "green",
                            in_place=False,
                            end="\n",
                            flush=True,
                        )
                    if exts is not None and os.path.splitext(file_url)[1] in exts:
                        urllib.request.urlretrieve(file_url, path)
                        file_count += 1
                        print_text(
                            "Downloaded: " + Fore.WHITE + "{}".format(file_name),
                            "green",
                            in_place=False,
                            end="\n",
                            flush=True,
                        )

                except KeyboardInterrupt:
                    # when CTRL+C is pressed during the execution of this script,
                    # bring the cursor to the beginning, erase the current line, and dont make a new line
                    print_text("✘ Got interrupted", "red", in_place=False)
                    sys.exit()
                except urllib.error.HTTPError as e:
                    if e.code == 403:
                        print_text("API Rate limit exceeded!!!", "red", in_place=True)
                        # download_from_github(file["html_url"], proxies, dir_out, flatten, exts=exts, file_count=file_count)
                    else:
                        print_text(e, "red", in_place=True)
                    sys.exit()
                except:
                    print_text("✘ Failed", "red", in_place=True)
                    sys.exit()
            else:
                download_from_github(
                    file["html_url"],
                    proxies,
                    dir_out,
                    flatten,
                    exts=exts,
                    file_count=file_count,
                )

    return file_count
