import re

from app.constants.idms import IDMS_NAME_SPLIT_REGEX


class IDMSUtils:
    """IDMS utils."""

    @staticmethod
    def name_to_snake_case(name: str) -> str:
        """
        Convert an IDMS item name to snake_case.

        :param name: Name.
        :return: Name formatted in snake_case.
        """

        return name.strip().lower().replace('-', '_')

    @staticmethod
    def name_to_camel_case(name: str) -> str:
        """
        Convert an IDMS item name to camelCase.

        :param name: Name.
        :return: Name formatted in camelCase.
        """

        text_stripped = name.strip()
        text_split = re.split(IDMS_NAME_SPLIT_REGEX, text_stripped)
        res = ''

        # Convert to camelCase
        if len(text_split) > 1:
            for i, s in enumerate(text_split):
                if i == 0:
                    res += s.lower()
                else:
                    res += s.title()
        else:
            res = re.sub(IDMS_NAME_SPLIT_REGEX, '', text_stripped).lower()

        return res
