from enum import Enum


class ExtractMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"
