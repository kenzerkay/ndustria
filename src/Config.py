import os

default_config_location = os.path.expanduser("~/.nd_config")

def load_config():

    if not os.path.isfile(default_config_location):
        return {}

    with open(default_config_location, "r") as config_file:

        config = {}

        for line in config_file.readlines():
            line = line.rstrip()
            tokens = line.split("=")
            config[tokens[0]] = tokens[1]


        return config