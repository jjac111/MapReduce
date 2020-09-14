import os
import random
import glob
from threading import Thread, Lock
from config import map_directory, reduce_directory, text_directory, fail_prob_coordinator, file, text_lines
from numpy import array_split
from pickle import dump, load
from json import dump as json_dump

partition_lock = Lock()
files_on_use = []


def clear():
    if not os.path.exists(map_directory):
        os.mkdir(map_directory)
    files = glob.glob(f'{map_directory}*')
    for f in files:
        os.remove(f)
    print(f'\'{map_directory}\' cleared.\n')

    if not os.path.exists(reduce_directory):
        os.mkdir(reduce_directory)
    files = glob.glob(f'{reduce_directory}*')
    for f in files:
        os.remove(f)
    print(f'\'{reduce_directory}\' cleared.\n')

    if not os.path.exists(text_directory):
        os.mkdir(text_directory)
    files = glob.glob(f'{text_directory}*')
    for f in files:
        os.remove(f)
    print(f'\'{text_directory}\' cleared.\n')


def split_text():
    with open(file) as f:
        num_files = int(file_len(f) / text_lines)
    with open(file) as f:
        split_lines = array_split(f.readlines(), num_files)
        for i, lines in enumerate(split_lines):
            with open(f'{text_directory}{(i + 1) * text_lines}.json', 'wb+') as ff:
                dump(' '.join(list(lines)), ff)
    print(f'Source text split into {i + 1} files.\n')


def clear_map():
    if not os.path.exists(map_directory):
        os.mkdir(map_directory)
    files = glob.glob(f'{map_directory}*')
    for f in files:
        os.remove(f)
    print(f'\'{map_directory}\' cleared.\n')


def try_fail(func):
    if random.uniform(0, 1) <= fail_prob_coordinator:
        func_name = func.__name__
        raise Exception('Coordinator node failed during ' + func_name + '!\nABORTING PROCESS')

    func()


def file_len(file):
    text = file.read()
    return len(text.split('\n'))


def not_dead(states):
    are_alive = True
    for s in states:
        if not s:
            are_alive = False
    return are_alive


def find_alive(threads, states):
    for i, (t, s) in enumerate(zip(threads, states)):
        if s:
            return i, t
    return None, None


def transfer_task(new_thread, task, old, new, func):
    if new_thread:
        new_thread = Thread(target=func, args=task)
        new_thread.start()
        print(f'Reduce node #{new} takes on #{old}\'s task')


def all_dead(states):
    dead = True
    for s in states:
        if s:
            dead = False
    return dead


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
        json_dump([(key, collected[key]) for key in sorted(collected)], f, indent=2)
