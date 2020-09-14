import os
from pickle import load
from json import dump
from config import reduce_directory


def collect():
    files = os.listdir(reduce_directory)

    collected = {}

    for file in files:
        with open(reduce_directory + file, 'rb') as f:
            reduced = load(f)

        collected.update(reduced)

    words = len(collected.keys())
    top = max(collected, key=collected.get)
    bot = min(collected, key=collected.get)

    print(f'The collected processed data has {words} different words.')
    print(f'The most common word is "{top}" with {collected[top]} occurrences.')
    print(f'One of the least common words is "{bot}" with {collected[bot]} occurrences.')

    with open('collected', 'w+') as f:
        dump([(key, collected[key]) for key in sorted(collected)], f, indent=2)
