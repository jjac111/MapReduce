import os
from threading import Thread
from config import nodes_reduce, map_directory
from numpy import array_split
from utils import find_alive, not_dead, transfer_task, all_dead
from reduce import reduce


def start_reduce():
    files = os.listdir(map_directory)
    file_groups = array_split(files, nodes_reduce)

    threads = [None] * len(file_groups)
    states = [True] * len(file_groups)

    for i, files in enumerate(file_groups):
        threads[i] = Thread(target=reduce, args=[files, i, states])
        threads[i].args = [files, i, states]
        threads[i].start()

    while True:
        for thread in threads:
            thread.join()

        if not_dead(states):
            break

        for i, (thread, state) in enumerate(zip(threads, states)):
            if not state:
                id_new, t = find_alive(threads, states)
                transfer_task(new_thread=t, task=thread.args, old=i, new=id_new, func=reduce)

        if all_dead(states):
            raise Exception('All reduce nodes have failed!')

    print('Reduce finished.\n')


