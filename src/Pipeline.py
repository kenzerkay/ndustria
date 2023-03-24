"""Defines the Pipeline class that represents the full analysis pipeline

A Pipeline is a singleton object that contains a list of Tasks and a list of Views
When Pipeline.run() is called, the pipeline will attempt to execute any Tasks that 
are indicated as ready by a call to Task.readyToRun(). A Task is ready if it either
has no dependencies, or all its dependencies have completed. 
Once all Tasks are complete, the Pipeline will then attempt to execute all its Views. 
Once all Views are complete, the Pipeline is done and the program will exit.

"""


from Task import Task
from Cache import Cache
from View import View

import os, sys, tracemalloc
from Logger import log, error

from mpi4py import MPI



class Pipeline:
    """Singleton class that contains a list of Tasks and Views to execute as part of a data analysis pipeline"""

    Tasks = [] 
    """List of all Task objects in this Pipeline"""
    
    Views = []
    """List of all View objects in this Pipeline"""

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Pipeline, cls).__new__(cls)

            # name the pipeline after the file that ran it w/o .py
            cls.instance.name = sys.argv[0].replace(".py","")
            cls.instance.cache = Cache()
            log(f"---\nPipeline {cls.instance.name} created with cache located at {cls.instance.cache.path}\n---\n")

            # TODO: Put some more thought into whether we should be using COMM_WORLD
            cls.instance.comm = MPI.COMM_WORLD

        return cls.instance

    def __init__(self):
        """This function intentionally left blank"""
        pass

    def addTask(self, 
        user_function, 
        args, 
        kwargs, 
        depends_on=None,
        match="most_recent"
    ):
        """Factory function for creating all new Tasks
        
        Arguments:
        user_function -- A user defined function that represents one stage of an analysis pipeline
        args -- a list of positional arguments to pass to user_function
        kwargs -- a dictionary of keyword arguments to apss to user_function
        depends_on -- a function, or list of functions that must be executed before this Task can be run
                      If only one function is given, the return value of that function will be passed 
                      as the first positional argument to user_function. If multiple functions with 
                      different names are given, a dictionary keyed by function names will be passed
                      as the first positional argument to user_function. If match="all", then 
                      a list containing all return values (ordered by Task creation order) will be 
                      passed as the first positional argument to user_function.
        match -- a string that tells ndustria how to assign dependencies. 
                 The options are:
                 most_recent -- Finds a Task that matches the given function name that was most recently added to the Pipeline
                 all -- Finds all Tasks that match the given function name
        """
        # figure out which Task this Task should depend on, if any
        dependencies = None
        if depends_on != None:

            if match == "most_recent":
                dependencies = self.MatchMostRecent(depends_on)
            elif match == "all":
                dependencies = self.MatchAll(depends_on)
            else:
                error(f"No dependency match solution for {match}")



        # create the new Task and append it to the Pipeline
        new_task = Task(
            len(self.Tasks),
            user_function, 
            args, 
            kwargs, 
            self,
            match=match,
            depends_on=dependencies)
        self.Tasks.append(new_task)

        if new_task.done:
            log(f"Task {new_task} will be skipped")
        else:
            log(f"Added new Task: {new_task}")

    def addView(self, 
        user_function, 
        args, 
        kwargs, 
        looks_at,
        match="most_recent",
        root_proc_only=False
    ):
        """Factory function for creating all new Views
        
        Arguments:
        user_function -- A user defined function that takes data created by a Task and produces a plot or other representation 
                         of the data
        args -- a list of positional arguments to pass to user_function
        kwargs -- a dictionary of keyword arguments to apss to user_function
        looks_at -- a function, or list of functions that must be executed before this View can be run
                      If only one function is given, the return value of that function will be passed 
                      as the first positional argument to user_function. If multiple functions with 
                      different names are given, a dictionary keyed by function names will be passed
                      as the first positional argument to user_function. If match="all", then 
                      a list containing all return values (ordered by Task creation order) will be 
                      passed as the first positional argument to user_function.
        match -- a string that tells ndustria how to assign dependencies. 
                 The options are:
                 most_recent -- Finds a Task that matches the given function name that was most recently added to the Pipeline
                 all -- Finds all Tasks that match the given function name
        root_proc_only -- If True, prevents this View from being executed on any process that does not have rank = 0
        """

        # figure out which Task this View should show
        dependencies = None
        if looks_at != None:

            if match == "most_recent":
                dependencies = self.MatchMostRecent(looks_at)
            elif match == "all":
                dependencies = self.MatchAll(looks_at)
            else:
                error(f"No dependency match solution for {match}")
            

        # create the new Task and append it to the Pipeline
        new_view = View(
            user_function, 
            args, 
            kwargs, 
            self,
            dependencies,
            root_proc_only,
            match=match)
        self.Views.append(new_view)

        log(f"Added new View: {new_view}")


    """ 
    Dependency matching functions
    These search the Task list for Tasks that match the function specified
    in the depends_on field
    """
    def MatchMostRecent(self, depends_on):
        """First and default Dependency strategy. Find the most recently added task that matches the depends_on function(s) """

        if not isinstance(depends_on, list):
            depends_on = [depends_on]

        dependencies = []
        for func in depends_on:
            # loop over the list in reverse since we want the 
            # most recently added 
            task = None
            task_found = False
            for i in range(len(self.Tasks)-1, -1, -1):
                task = self.Tasks[i]
                if task.user_function.__name__ == func.__name__:
                    task_found = True
                    dependencies.append(task)
                    break
            if not task_found:
                error(f"No matching Task found for '{func.__name__}'")
        return dependencies
    
    def MatchAll(self, depends_on):
        """ Find all Tasks that match the depends_on function(s) """

        if not isinstance(depends_on, list):
            depends_on = [depends_on]

        dependencies = []
        task_found = False # set to True if at least one Task is found
        for func in depends_on:
            for task in self.Tasks:
                if task.user_function.__name__ == func.__name__:
                    dependencies.append(task)
                    task_found = True
            if not task_found:
                error(f"No matching Task found for '{func.__name__}'")
        return dependencies


    """
    Parallel utility functions
    """
    def getCommRank(self):
        """Convenience method to get MPI rank"""
        return self.comm.Get_rank()

    def getCommSize(self):
        """Convenience method to get MPI comm size"""
        return self.comm.Get_size()
    
    def isRoot(self):
        """A process is considered the 'root' process iff rank == 0"""
        return self.comm.Get_rank() == 0

    """
    The main Task running function
    """
    @staticmethod
    def run(
        rerun=False, 
        parallel=False,
        dryrun=False,
        timeit=True,
        memcheck=False ):
        """
        Runs a pipeline composed of ndustria Tasks and Views

        Step 1. Write functions that represent each stage of your analysis pipeline
        Step 2. Decorate your functions with the addTask and addView decorators as appropriate
        Step 3. Call your decorated functions with desired arguments
        Step 4. Call this function to run the pipeline

        Keyword arguments:
        rerun -- If True, removes any previous Task results from the cache. Causes every Task to run from scratch.
        parallel -- If True, uses a round robin approach to assign Tasks to multiple processes and runs them in parallel
        dryrun -- If True, skips running Tasks but does everything else, including creating log files. Used to test complex pipelines
        timeit -- If True, keeps track of wallclock time of each Task. These data will be output to a csv file in the cache. Set to True by default due to low overhead
        memcheck -- If True, collects initial, peak, and final memory usage of each Task. These data will be output to a csv file in the cache. Can have high overhead if you allocate a lot of small objects
        """

        pipe = Pipeline()

        pipe.timeit = timeit
        pipe.memcheck = memcheck
        pipe.dryrun = dryrun

        if parallel:
            log(f"Initializing parallel run with {pipe.getCommSize()} processes")

        if rerun:
            pipe.clearCache()

        if memcheck:
            tracemalloc.start(25) # TODO: Move this to .env

        pipe.comm.Barrier()

        run_this_iteration = []
        waiting = [task for task in pipe.Tasks if not task.done]

        num_waiting = len(waiting)

        log(f"---\n Starting a run with {num_waiting} tasks.\n---\n")

        MAX_ITERATIONS = 10000
        iterations = 0
        while num_waiting > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [task for task in waiting if task.readyToRun()]


            #TODO: May want to put a try catch around this to 
            # help it fail gracefully
            for i, task in enumerate(run_this_iteration):

                if parallel:
                    # TODO: this works so long as the dependent Tasks end up 
                    # running on the same process as their dependencies
                    # otherwise this will fail
                    # lots to consider in fixing this
                    # it might be ok to distribute Task results to all processes
                    # so long as results*comm_size is small compared to peak memory and that might be easier
                    # than trying to associate particular Task dependency chains
                    # with a particular process

                    # round robin distribute Tasks to processes
                    if i % pipe.getCommSize() == pipe.getCommRank():
                        print(f"[Rank {pipe.getCommRank()}] running:" + str(task))
                        task.run()
                    else:
                        # Mark this Task done on other processes
                        task.done = True
                else:
                    task.run()

            pipe.comm.Barrier()

            waiting = [task for task in pipe.Tasks if not task.done]

            print(f"[Rank {pipe.getCommRank()}] waiting on {len(waiting)} Tasks")

            log(f"---\nIteration {iterations} finished. {len(waiting)} Tasks left\n---")
            if len(waiting) != 0 and num_waiting <= len(waiting):
                error("Looks like the last run didn't complete any Tasks. Check your script for missing dependencies. Exiting")

            num_waiting = len(waiting)
            
        # end main while loop

        log(f"Finished all tasks after {iterations} iterations")

        run_this_iteration = []
        waiting = [view for view in pipe.Views]

        while len(waiting) > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [view for view in waiting if view.readyToRun()]

            for view in run_this_iteration:
                view.run()

            waiting = [view for view in pipe.Views if not view.shown]

        # end Views while loop

        if pipe.timeit:
            # save it to cache for internal use
            timing_data_file = os.path.join(pipe.cache.path, f"{pipe.name}_timing.csv")
            with open(timing_data_file, "w") as timing_data:
                for task in pipe.Tasks:
                    timing_data.write(f"{task.user_function.__name__}, {task.wallTime}\n")

        if pipe.memcheck:
            memcheck_data_file = os.path.join(pipe.cache.path, f"{pipe.name}_memcheck.csv")

            with open(memcheck_data_file, "w") as memcheck_data:
                for task in pipe.Tasks:
                    memcheck_data.write(f"{task.user_function.__name__}, {task.initial_mem}, {task.final_mem}, {task.peak_mem}\n")

        log("All done.")
          

    @staticmethod
    def printCacheInfo():
        """Prints the cache info file to console. Not supported on Windows"""

        pipe = Pipeline()

        # because windows users can fucking die
        os.system(f"cat {pipe.cache.info_file}")

    @staticmethod
    def printLog():
        """Prints the log file to console. Not supported on Windows"""
        pipe = Pipeline()

        # because windows users can fucking die
        os.system(f"cat {pipe.cache.log_file}")

    @staticmethod
    def clearCache():
        """Deletes all results in the cache that match the Task list. Asks for permission first."""
        pipe = Pipeline()

        if pipe.isRoot():

            files_to_remove = []
            for task in pipe.Tasks:
                filepath = os.path.join(pipe.cache.path, task.getFilename())
                if os.path.isfile(filepath):
                    files_to_remove.append(filepath)

            if len(files_to_remove) > 0:
                files_to_remove = "\n".join(files_to_remove)
            else:
                print("Nothing found in the cache. No need to clear.")
                return

            i = 0
            while True:
                print(f"\n[Caution] About to delete the following files:\n{files_to_remove}")
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
                elif i >= 6:
                    print("Ok thats enough. Can't risk you deleting your parent's files. Exiting.")
                    exit()
                else:
                    print("Please answer with 'y' or 'n'")

                i += 1
            # end while True

            for task in pipe.Tasks:
                pipe.cache.remove(task)
        pipe.comm.Barrier()
        
        # reset the Cache
        pipe.cache.setPath()

        # reset the state of all tasks 
        for task in pipe.Tasks:
            task.done = False
            task.result = None

    def getAllHashCodes(self):
        """Utility function that isn't actually being used for anything right now. Might delete later idk."""
        return [task.getHashCode() for task in self.Tasks]
        