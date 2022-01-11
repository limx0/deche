__version__ = "0.0.22"

from donfig import Config


config = Config("deche")

from deche.core import *  # noqa: F401, F403
