from importlib.metadata import version

from donfig import Config


__version__ = version(__name__)

config = Config("deche")

from deche.core import *  # noqa: F401, F403
