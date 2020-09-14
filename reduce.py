import random
from config import map_directory, reduce_directory, fail_prob_reduce
from pickle import load, dump


def reduce(to_reduce, id, states):
    reduced = {}
    states[id] = False
    for f in to_reduce:
        if random.uniform(0, 1) <= fail_prob_reduce:
            print(f'Reduce node #{id} failed!')
            return

        with open(map_directory + f, 'rb') as file:
            mapped = load(file)

        for word, count in mapped.items():
            reduced[word] = reduced.get(word, 0) + count

    with open(f'{reduce_directory}{id + 1}', 'wb+') as file:
        dump(reduced, file)

    states[id] = True
