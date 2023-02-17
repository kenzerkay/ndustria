import inspect, hashlib
from Logger import log, warn

class Task:

    def __init__(self, id,
        user_function, 
        args, 
        kwargs, 
        pipeline, 
        depends_on=None
    ):
        
        self.user_function = user_function
        self.args = args
        self.kwargs = kwargs
        self.pipeline = pipeline

        # True if the Task has no dependencies
        self.indepedent = False

        if isinstance(depends_on, list):
            self.depends_on = depends_on
        elif depends_on == None:
            self.depends_on = []
            self.indepedent = True
        else :
            self.depends_on = [depends_on]

        # name of the file where this Task's data is stored
        self.filename = ""

        # assign this Task its hashcode
        self.hashcode = ""
        self.getHashCode()

        # reference to the data product this Task makes
        self.result = None
        self.done = False

        # figure out if this Task has a result in cache already and load it
        if self.pipeline.cache.exists(self):
            self.done = True
            self.getResult()
            

    # end __init__      
        

    def __str__(self):
        debug_string = f"{self.user_function.__name__}("
        for i, a in enumerate(self.args):
            debug_string += str(a)

            if i == len(self.args)-1 and len(self.kwargs) == 0:
                debug_string += ")"
            else:
                debug_string += ", "

        i = 0
        for k,v in self.kwargs.items():
            debug_string += f"{str(k)}={str(v)}"

            if i == len(self.kwargs.items())-1:
                debug_string += ")"
            else:
                debug_string += ", "
            i+=1

        if not debug_string.endswith(")"):
            debug_string += ")"

        if not self.indepedent:
            debug_string += f" which depends on {str(self.depends_on)}"

        return debug_string

    def __repr__(self):
        return str(self)

    def run(self):

        log(f"Running {self}")

        if self.indepedent:
            self.result = self.user_function(*self.args, **self.kwargs)            
        else:
            data = self.getDependencyData()
            self.result = self.user_function(data, *self.args, **self.kwargs)
            
        self.pipeline.cache.save(self)
        self.done = True

    def getDependencyData(self):

        if len(self.depends_on) == 1:
            return self.depends_on[0].getResult()
        
        all_results = []
        for task in self.depends_on:
            all_results.append( task.getResult() )

        return all_results

    def getFilename(self):

        if self.filename != "":
            return self.filename
        else:
            return self.getHashCode()

    def getResult(self):
        """
        Gets the result of this task if one exists
        Will return None 
        """

        if not self.done:
            warn("Task had getResult called before it was run. Result will be None")
            return None

        if(self.result is not None):
            return self.result
        else:
            self.result = self.pipeline.cache.load(self)

        return self.result

    def readyToRun(self):
        """
        Determines whether or not this Task is ready to be run by running
        through its dependencies and return true if they are all marked "done"
        """

        if self.indepedent:
            return True
        
        for task in self.depends_on:
            if not task.done:
                return False
        return True

    def getHashCode(self):

        if self.hashcode != "":
            return self.hashcode

        # get the source code of the operation
        source = inspect.getsource(self.user_function)

        #TODO: Finish function that removes lines with comments
        def remove_all_comments(str):
            pass

        def remove_all_whitespace(str):
            ws_chars = [' ', '\t', '\n']
            for char in ws_chars:
                str = str.replace(char, '')
            return str

        # TODO: Perform a check to see if any arguments 
        # are missing a str implementation, if possible
        # any arguments that are pointers to objects 
        # will cause cache invalidation due to the default
        # str representation including the address to the object
        # which is almost certainly going to be unique to a 
        # given run of the code
        # For that reason, arguments passed in to an NDustrio task
        # must be able to be uniquely represented by a call to str()
        def append_args(target, args, kwargs):
            for a in args:
                target += str(a)

            for k,v in kwargs.items():
                target += str(k)+str(v)
            return target

        # scrap the whitespace to prevent unnecessary 
        # re-queries
        source_no_ws = remove_all_whitespace(source)

        # concatenate it with the arguments
        if self.indepedent:
            target = append_args(source_no_ws, self.args, self.kwargs)
        # if we have dependencies, add the hashcodes/filenames of those dependencies
        else:
            add_hashes = ""
            for task in self.depends_on:
                add_hashes += task.getHashCode()
            
            target = append_args(source_no_ws+add_hashes, self.args, self.kwargs)
        
        
        # convert string to a hash
        hash = hashlib.md5(target.encode()).hexdigest()
        self.hashcode = hash

        #debug(f"Created hash {hash} for {self} from string {target}")
        return hash