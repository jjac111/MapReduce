import random
from config import fail_prob_coordinator
from collect import collect
from start_map import start_map
from start_reduce import start_reduce
from utils import clear, try_fail, split_text



try_fail(clear)

try_fail(split_text)

try_fail(start_map)

try_fail(start_reduce)

try_fail(collect)
