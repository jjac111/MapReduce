import time
from start_map import start_map
from start_reduce import start_reduce
from utils import clear, try_fail, split_text, collect
from pickle import load


start_time = time.time()

try_fail(clear)

try_fail(split_text)

try_fail(start_map)

try_fail(start_reduce)

try_fail(collect)

print(f"\nTotal execution time: {round(time.time() - start_time, 2)} seconds.")
