import pickle, os
from log import debug, error

CACHE_PATH = "./temp"

class Cache:

    def __init__(self, path):
        self.setPath(path)


    def exists(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())

        cache_hit = os.path.exists(cache_fname)
        
        return cache_hit 

    def load(self, task):
        
        cache_fname = os.path.join(self.path, task.getFilename())

        # if we have a previous result, serve that up
        try:
            with open(cache_fname, 'rb') as f:
                debug(f"Found cache result for {cache_fname}")
                result = pickle.load(f)
        except FileNotFoundError as e:
            debug(f"No cache result found for {cache_fname}")
            raise 
        else:
            task.result = result

            return result

    def save(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())
        
        with open(cache_fname, 'wb') as f:
            pickle.dump(task.result, f, protocol=0)

        debug(f"Saved result of {task} to {cache_fname}")


    def remove(self, task):
        cache_fname = os.path.join(self.path, task.getFilename())
        try: 
            os.remove(cache_fname)
        except FileNotFoundError as e:
            pass



    def setPath(self, new_path):
        self.path = os.path.abspath(new_path)
        if not os.path.exists(self.path):
                os.mkdir(self.path)

    

    