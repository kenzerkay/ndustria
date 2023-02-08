

class View:

    def __init__(self, user_function, args, kwargs, pipeline, task):
        self.user_function = user_function
        self.args = args
        self.kwargs = kwargs
        self.pipeline = pipeline
        self.task = task
        self.shown = False

        # name of the file where this Task's data is stored
        self.filename = ""

        # keep a hash for every Task so we know whether to invalidate the cache
        self.hashcode = ""

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

        debug_string += f" which views {str(self.task)}"

        return debug_string

    def __repr__(self):
        return str(self)

    def run(self):

        data = self.getDependencyData()
        self.user_function(data, *self.args, **self.kwargs)
            
        self.shown = True


    def readyToRun(self):
        """
        Determines whether or not this Task is ready to be run by running
        through its dependencies and return true if they are all marked "done"
        """
        
        return self.task.done


    # TODO: make this work for multiple dependencies
    def getDependencyData(self):

        return self.task.getResult()