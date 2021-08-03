def match_to_str(match) -> str:
    """
    Get matched string from regex match.

    :param match: Regex match.
    :return: Match as string.
    """

    span = match.span()
    return match.string[span[0]:span[1]]
