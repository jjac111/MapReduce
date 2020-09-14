import os
from numpy import array_split
from threading import Thread
from config import nodes_map, text_directory
from utils import clear_map, not_dead
from map import map


def start_map():
    files = os.listdir(text_directory)
    file_groups = array_split(files, nodes_map)

    threads = [None] * len(file_groups)
    states = [True] * len(file_groups)

    locks = {}

    while True:
        for i, files in enumerate(file_groups):
            threads[i] = Thread(target=map, args=[files, i, states, locks])
            threads[i].args = [files, i, states]
            threads[i].start()

        for thread in threads:
            thread.join()

        if not_dead(states):
            break

        clear_map()
        print('Restarting mapping tasks.\n')

    print('Mapping finished.\n')
