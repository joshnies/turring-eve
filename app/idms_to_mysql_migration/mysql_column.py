class MySQLColumn:
    """MySQL column model."""

    def __init__(
            self,
            name: str,
            var_type: str,
            length: int,
            length_1: int = None,
            length_2: int = None,
            default_value=None,
    ):
        self.name = name
        self.var_type = var_type
        self.length = length
        self.length_1 = length_1
        self.length_2 = length_2
        self.default_value = default_value
