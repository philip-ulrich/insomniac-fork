import re
import sys
import random
# Import warnings and enable DeprecationWarning's for parse function
import warnings
warnings.simplefilter('always', DeprecationWarning)

if sys.version_info[0] == 2:
    # If Python 2 use unicode character conversion
    chr = unichr

def _replace_string(match):
    """
    Function to replace the spintax with a randomly chosen string
    :param match object:
    :return string:
    """
    global spintax_seperator, random_string
    test_string = re.sub(spintax_seperator, lambda x: x.group(1)+random_string, match.group(2))
    split_strings = re.split(random_string, test_string)
    random_picked = random.choice(split_strings)
    return match.group(1) + random_picked + ['', match.group(3)][random_picked==split_strings[-1]]


def spin(string, seed=None):
    """
    Function used to spin the spintax string
    :param string:
    :param seed:
    :return string:
    """

    # As look behinds have to be a fixed width I need to do a "hack" where
    # a temporary string is used. This string is randomly chosen. There are
    # 1.9e62 possibilities for the random string and it uses uncommon Unicode
    # characters, that is more possibilerties than number of Planck times that
    # have passed in the universe so it is safe to do.
    characters = [chr(x) for x in range(1234, 1368)]    
    global random_string
    random_string = ''.join(random.sample(characters, 30))
    
    # If the user has chosen a seed for the random numbers use it
    if seed is not None:
        random.seed(seed)

    # Regex to find spintax seperator, defined here so it is not re-defined
    # on every call to _replace_string function
    global spintax_seperator
    spintax_seperator = r'((?:(?<!\\)(?:\\\\)*))(\|)'
    spintax_seperator = re.compile(spintax_seperator)

    # Regex to find all non escaped spintax brackets
    spintax_bracket = r'(?<!\\)((?:\\{2})*)\{([^}{}]+)(?<!\\)((?:\\{2})*)\}'
    spintax_bracket = re.compile(spintax_bracket)

    # Need to iteratively apply the spinning because of nested spintax
    while True:
        new_string = re.sub(spintax_bracket, _replace_string, string)
        if new_string == string:
            break
        string = new_string

    # Replaces the literal |, {,and }.
    string = re.sub(r'\\([{}|])', r'\1', string)
    # Removes double \'s
    string = re.sub(r'\\{2}', r'\\', string)

    return string


def parse(string, seed=None):
    """
    Old function used will be removed use spin() instead
    :param string:
    :param seed:
    :return list:
    """
    warnings.warn(
        "This function (parse) is being depreciated, please use spin(string)",
        DeprecationWarning
    )
    return [spin(string, seed)]
