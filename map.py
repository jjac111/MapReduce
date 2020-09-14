import re
import os
import random
from threading import Lock
from pickle import dump, load
from config import text_directory, map_directory, fail_prob_map, fail_prob_partition



def map(to_map, id, states, locks):
    dictionary = {}
    states[id] = False
    for file in to_map:
        if random.uniform(0, 1) <= fail_prob_map:
            print(f'Map node #{id} failed!')
            return

        with open(text_directory + file, 'rb') as f:
            text = load(f)

        for word in text.split(' '):
            word = re.sub(r'\W+', '', word)

            if not word:
                continue

            dictionary[word] = dictionary.get(word, 0) + 1

    try:
        partition_store(dictionary, locks)

    except Exception as e:
        raise e
    states[id] = True


def partition_store(dictionary, locks):
    if random.uniform(0, 1) <= fail_prob_partition:
        print(f'Map node #{id} failed during partitioning!')
        raise Exception

    to_store = {}
    for word, count in dictionary.items():
        letter = word[0].lower()
        to_store[letter] = to_store.get(letter, {})

        to_store[letter][word] = to_store[letter].get(word, 0) + count

    for letter, d in to_store.items():
        filepath = f'{map_directory}{letter}'
        locks[filepath] = locks.get(filepath, Lock())

        # Lock files to be written so multiple Maps have to wait to concurrently modify a file.
        locks[filepath].acquire()

        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                read_pickle = load(f)
                for key, value in d.items():
                    read_pickle[key] = read_pickle.get(key, 0) + value
                f.close()
            with open(filepath, 'wb') as f:
                dump(read_pickle, f)
        else:
            with open(filepath, 'wb') as f:
                dump(d, f)

        # Release the lock
        locks[filepath].release()
