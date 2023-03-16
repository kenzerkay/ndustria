import pickle, os
from tabulate import tabulate
from Logger import log, error, setLogFile

# including numpy support
import numpy as np

CACHE_PATH = "./temp"

class Cache:

    def __init__(self, path):
        self.setPath(path)
        self.headers = [
            "Task",
            "File size (bytes)",
            "File name in cache"
        ]
    # end init



    def writeCacheInfo(self):

        table_out = []

        for k,v in self.table.items():
            table_out.append([v[0], v[1], k])

        with open(self.info_file, "w") as info:
            info.write(f"\nCache location: {self.path}\n\n")
            info.write(tabulate(table_out, headers=self.headers))
            info.write("\n")

        with open(self.table_file, "wb") as cache_data:
            pickle.dump(self.table, cache_data)
    # end writeCacheInfo


    def exists(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())

        cache_hit = os.path.exists(cache_fname)
        
        return cache_hit 
    # end exists

    def load(self, task):
        
        cache_fname = os.path.join(self.path, task.getFilename())

        # if we have a previous result, serve that up
        try:
            with open(cache_fname, 'rb') as f:
                log(f"Found cache result for {cache_fname}")
                result = pickle.load(f)
        except FileNotFoundError as e:
            error(f"No cache result found for {cache_fname}")
        else:
            task.result = result

            return result
    # end load

    def save(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())

        result = task.getResult()
        
        with open(cache_fname, 'wb') as f:
            pickle.dump(task.result, f)

        file_size = os.stat(cache_fname).st_size
        self.table[os.path.basename(cache_fname)] = (
            str(task),
            file_size,
        )
            
        self.writeCacheInfo()
        log(f"Saved result of {task} to {cache_fname}")
    # end save


    def remove(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())
        try: 
            os.remove(cache_fname)
        except FileNotFoundError as e:
            pass
    # end remove

    def clear(self):
        
        i = 0
        while True:
            print(f"\n[Caution] About to delete all files in {self.path}")
            answer = input("Is this ok? [y/n]\n")
            if answer == "y":
                print("Ok. Deleting files.")
                break
            elif answer == "n":
                print("Got it. Your files are safe. Exiting.")
                exit()
            elif i == 3:
                print("Is there a cat walking on your keyboard right now?")
            elif i == 4:
                print("My cats do that a lot.")
            elif i == 5:
                print("Hi kitty! You are very cute! (=^･ω･^=)")
            elif i == 6:
                print("Ok thats enough. Can't risk you deleting your parent's files. Exiting.")
                exit()
            else:
                print("Please answer with 'y' or 'n'")

            i += 1
        # end while True

        # TODO: Rework this once the cache is better aware of which files 
        # exist within it. The cache should specifically delete only those files
        os.system(f"rm {self.path}/*")

        # Re-initialize the cache with empty files
        self.setPath(self.path)

    # end clear

    def setPath(self, new_path):

        # Function to create file if does not exist
        def touch(file):

            if os.path.isfile(file): return

            with open(file, 'w'):  
                pass
            # end with
        # end touch

        # same thing for directories
        def touchDir(path):
            if os.path.exists(path): return

            os.mkdir(path)
        # end touchDir


        self.path = os.path.abspath(new_path)
        touchDir(self.path)

        self.info_file = os.path.join(self.path, "cache_info")
        touch(self.info_file)

        
        self.log_file = os.path.join(self.path, "last_run.log")
        setLogFile(self.log_file)
        touch(self.log_file)

        self.table_file = os.path.join(self.path, "cache_data")
        touch(self.table_file)
        self.loadTable()

    # end setPath
    
  
    def loadTable(self):

        try:
            with open(self.table_file, 'rb') as f:
                log(f"Reloading cache state from {self.table_file}")
                self.table = pickle.load(f)
        except EOFError as eof:
            # There's nothing in the file (probably because it was just created)
            # so initialize the table to an empty dictionary and move on
            self.table = {}
        except FileNotFoundError as fnf:
            error(f"File not found: {self.table_file}. Exiting. ")

    # end loadTable

        

    