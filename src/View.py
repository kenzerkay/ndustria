from .Task import Task

class View:

    def __init__(
            self, 
            user_function, 
            args, kwargs, 
            pipeline, 
            root_only
        ):
        self.user_function = user_function
        self.args = args
        self.kwargs = kwargs
        self.pipeline = pipeline
        self.root_only = root_only
        self.shown = False

        # name of the file where this Task's data is stored
        self.filename = ""

        # keep a hash for every Task so we know whether to invalidate the cache
        self.hashcode = ""

        self.tasks = [
            a for a in self.args if Task.isTask(a)
        ]


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

        return debug_string

    def __repr__(self):
        return str(self)

    def getString(self):
        """Returns a truncated string representation of this Task for human-readable logging"""

        task_string = str(self)

        if (len(task_string) > 80):
            return task_string[:80] + "..."
        else:
            return task_string

    def run(self):

        if self.root_only and not self.pipeline.isRoot():
            self.shown = True
            return
        
        arguments = Task.parseArgs(self.args)

        self.user_function(*arguments, **self.kwargs)
            
        self.shown = True


    def readyToRun(self):
        """
        Determines whether or not this Task is ready to be run by running
        through its dependencies and return true if they are all marked "done"
        """
        
        for task in self.tasks:
            if not task.done:
                return False
        return True

