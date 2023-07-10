import pickle, os
from tabulate import tabulate
from .Logger import log, error, setLogFile
from dotenv import load_dotenv

# including numpy support
import numpy as np

CACHE_PATH = "./temp"

class Cache:

    def __init__(self, path=None):

        if path == None:
            load_dotenv()
            path = os.environ.get("NDUSTRIA_CACHE_DIR")

        # if the environment variable has not been set, prompt the user
        # to run first time setup
        if path == None or path == "":
            print("\nHello! It looks like the NDUSTRIA_CACHE_DIR environment variable has not been set.\nPlease run ndustria's first time setup with 'ndustria -s'")
            exit()

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
                result = pickle.load(f)
        except FileNotFoundError as e:
            error(f"""No cache result found for {cache_fname}.
Task Information:
{task}
""")
        else:
            task.result = result

            return result
    # end load

    def save(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())

        result = task.getResult()

        result_is_external_file = False
        if type(result) == str:
            result_is_external_file = True
        
        with open(cache_fname, 'wb') as f:
            pickle.dump(task.result, f)

        file_size = os.stat(cache_fname).st_size
        self.table[os.path.basename(cache_fname)] = (
            task.getString(),
            file_size,
        )
            
        self.writeCacheInfo()
        if result_is_external_file:
            log(f"Saved result of {task.getString()} to {result}")
        else:
            log(f"Saved result of {task.getString()} to {cache_fname}")

    # end save


    def remove(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())
        try: 
            os.remove(cache_fname)
        except FileNotFoundError as e:
            pass
    # end remove

    def setPath(self, new_path=None):

        # if no new path, just reset the old one
        if new_path == None:
            new_path = self.path

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
                #log(f"Reloading cache state from {self.table_file}")
                self.table = pickle.load(f)
        except EOFError as eof:
            # There's nothing in the file (probably because it was just created)
            # so initialize the table to an empty dictionary and move on
            self.table = {}
        except FileNotFoundError as fnf:
            error(f"File not found: {self.table_file}. Exiting. ")

    # end loadTable

    def getFullPathToTask(self, task):

        return os.path.join(self.path, task.getFilename())

    