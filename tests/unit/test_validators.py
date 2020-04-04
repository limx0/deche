import time

from deche.core import cache, tokenize
from deche.test_utils import func, identity, func_ttl_expiry, func_ttl_expiry_append, tmp_fs
from deche.types import FrozenDict


