"""A Task is a the smallest unit of work performed by an analysis Pipeline

A Task is a the smallest unit of work performed by an analysis Pipeline. 
Tasks must be deterministic (returns a the same result for the same arguments )
and atomic (performs all its work without depending on another Task being run concurrently).

A Task can either be independent (doesn't rely on the completion of any previous Task)
or dependent (relies on the previous completion of one or more Tasks).

When a Task is created, it gets passed a function and set of positional and keyword arguments.
This function will be executed once the Pipeline it belongs to has determined that the Task 
is ready to run. 

-----------------------------------------------------------------------------------------
IMPORTANT
-----------------------------------------------------------------------------------------

Any arguments passed to a Task's user_function must have a unique string representation
given by the str() function. This is due to the default behavior of the str() function
for classes in Python, which returns the memory address of the object. That address is almost
always unique to a specific run of the code and therefore produces indeterministic hashes
even though the underlying data might be exactly the same.
-----------------------------------------------------------------------------------------

Once the Task completes, the return value of its function is saved to a file in the ndustria
Cache. The filename is determined by taking a hash of the function's source code, arguments, 
and the hashcodes of any dependencies. This way, if the code changes, the arguments change, 
or any of the dependencies change, the Cache will not be able to find a result for it
and the Task will be rerun. 
"""

import inspect, hashlib, time, tracemalloc
from .Logger import log, warn

class Task:
    """A Task is a the smallest unit of work performed by an analysis Pipeline"""
    def __init__(self, id,
        user_function, 
        args, 
        kwargs, 
        pipeline
    ):
        """Initializes a new Task. Should not be called directly. Instead use the @AddTask decorator.

        Arguments:
        user_function -- A user defined function that executes when this Task is run. Its return value is saved to a file in the Cache
        args -- A list of positional arguments to pass to the function. 
        kwargs -- A list of keyword arguments to pass to the function.
        pipeline -- A reference to the pipeline this Task belongs to. Not strictly necessary since the Pipeline is a static singleton but whatev
        """
        
        self.user_function = user_function
        self.args = args
        self.kwargs = kwargs
        self.pipeline = pipeline

        # Run statstics i.e. wall clock time and memory
        self.wallTime = 0
        self.initial_mem = 0
        self.peak_mem = 0
        self.final_mem = 0

        # True if the Task has no dependencies
        self.indepedent = True

        # any arguments that are Task objects are dependencies that need to be
        # tracked by the dependencies list
        self.dependencies = []
        for a in self.args:
            if Task.isTask(a):
                self.indepedent = False
                self.dependencies.append(a)

        # name of the file or files where this Task's data is stored
        self.filename = None

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
        """Returns the Task name, arguments, and dependencies as a string."""
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
            debug_string += f" which depends on {str(self.dependencies)}"

        return debug_string

    def __repr__(self):
        """Just calls str() cuz I'm lazy."""
        return str(self)
    
    def getString(self):
        """Returns a truncated string representation of this Task for human-readable logging"""

        task_string = str(self)

        if (len(task_string) > 50):
            return task_string[:50] + "..."
        else:
            return task_string

    def __iter__(self):

        if self.done:
            return self.getResult().__iter__()

        if not self.readyToRun():
            raise TaskNotReadyError(self)
        
        self.run()

        return self.getResult().__iter__()
        


    def run(self):
        """Runs the Task by calling its user_function with the supplied arguments and any dependency data"""

        log(f"Running {self.getString()}")

        arguments = []
        for a in self.args:

            arg_is_task = Task.isTask(a)

            arg_is_list_of_tasks = type(a) == list and Task.isTask(a[0])

            if arg_is_task:
                arguments.append(a.getResult())
            elif arg_is_list_of_tasks: 
                new_list = [t.getResult() for t in a]
                arguments.append(new_list)
            else:
                arguments.append(a)

        if self.pipeline.timeit: 
            start = time.time()

        if self.pipeline.memcheck:
            self.initial_mem, self.peak_mem = tracemalloc.get_traced_memory()

        ###################################################################
        # Run the actual function
        ###################################################################
        self.result = self.user_function(*arguments, **self.kwargs)   
        
        if self.pipeline.timeit: 
            self.wallTime = time.time() - start

        if self.pipeline.memcheck:
            self.final_mem, self.peak_mem = tracemalloc.get_traced_memory()

       
            
        ###################################################################
        # Save the result
        ###################################################################
        self.done = True
        self.pipeline.cache.save(self)

    def getFilename(self):
        """Returns the filename in the cache that this Task saves to. May not necessarily be the same as the Task hashcode.
        """
        return self.getHashCode()

    def getResult(self):
        """ Gets the result of this task if one exists. Will return None if no result exists.
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
        """Determines whether or not this Task is ready to be run by running through its dependencies and return true if they are all marked "done"

        Tasks are marked "done" when they are finished running, or when they were sent to run on another process. 
        """

        if self.indepedent:
            return True
        
        for task in self.dependencies:
            if not task.done:
                return False
        return True

    def getHashCode(self):
        """Converts the source code, arguments, and any dependency hash codes to a hash with the md5 algorithm.
        
        This hash gets used as a filename to save Task results in the ndustria Cache. It can be considered a unique
        identifier of a specific instance of a Task running with a particular set of parameters.
        """

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
        # For that reason, arguments passed in to an ndustria task
        # must be able to be uniquely represented by a call to str()
        def append_args(target, args, kwargs):
            for a in args:
                target += str(a)

            for k,v in kwargs.items():
                target += str(k)+str(v)
            return target

        # Q: Is this actually a good idea? Whitespace changes code behavior in python
        # scrap the whitespace to prevent unnecessary 
        # re-queries
        source_no_ws = remove_all_whitespace(source)

        # concatenate it with the arguments
        if self.indepedent:
            target = append_args(source_no_ws, self.args, self.kwargs)
        # if we have dependencies, add the hashcodes/filenames of those dependencies
        else:
            add_hashes = ""
            for task in self.dependencies:
                add_hashes += task.getHashCode()
            
            target = append_args(source_no_ws+add_hashes, self.args, self.kwargs)
        
        
        # convert string to a hash
        hash = hashlib.md5(target.encode()).hexdigest()
        self.hashcode = hash

        #debug(f"Created hash {hash} for {self} from string {target}")
        return hash
    

    @staticmethod
    def isTask(task):

        return task.__class__.__name__ == "Task"
    

class TaskNotReadyError(Exception):

    def __init__(self, task):
        self.task = task
        self.message = "Task attempted to run before it was ready."