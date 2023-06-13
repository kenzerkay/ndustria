"""Defines the Pipeline class that represents the full analysis pipeline

A Pipeline is a singleton object that contains a list of Tasks and a list of Views
When Pipeline.run() is called, the pipeline will attempt to execute any Tasks that 
are indicated as ready by a call to Task.readyToRun(). A Task is ready if it either
has no dependencies, or all its dependencies have completed. 
Once all Tasks are complete, the Pipeline will then attempt to execute all its Views. 
Once all Views are complete, the Pipeline is done and the program will exit.

"""

import sys
from .Task import Task
from .Cache import Cache
from .View import View
from .Logger import log, error
import os, sys, tracemalloc

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

            # TODO: Put some more thought into whether we should be using COMM_WORLD
            cls.instance.comm = MPI.COMM_WORLD

            if cls.instance.isRoot():
                log(f"---\nPipeline {cls.instance.name} created with cache located at {cls.instance.cache.path}\n---\n")


        return cls.instance

    def __init__(self):
        self.rerun=False
        self.parallel=False
        self.dryrun=False
        self.timeit=True
        self.memcheck=False

    def addTask(self, 
        user_function, 
        args, 
        kwargs,
        rerun=False
    ):
        """Factory function for creating all new Tasks
        
        Arguments:
        user_function -- A user defined function that represents one stage of an analysis pipeline
        args -- a list of positional arguments to pass to user_function
        kwargs -- a dictionary of keyword arguments to pass to user_function
        """

        # create the new Task and append it to the Pipeline
        new_task = Task(
            len(self.Tasks),
            user_function, 
            args, 
            kwargs, 
            self,
            rerun=rerun)
        self.Tasks.append(new_task)

        if self.isRoot(): 
            if new_task.done:
                log(f"[Cache hit!] Task {new_task.getString()} will be skipped")
            else:
                log(f"[Added Task] {new_task.getString()}")
            # end if
        # end if


        return new_task

    def addView(self, 
        user_function, 
        args, 
        kwargs,
        root_only=False
    ):
        """Factory function for creating all new Views
        
        Arguments:
        user_function -- A user defined function that takes data created by a Task and produces a plot or other representation 
                         of the data
        args -- a list of positional arguments to pass to user_function
        kwargs -- a dictionary of keyword arguments to apss to user_function
        root_only -- If True, prevents this View from being executed on any process that does not have rank = 0
        """ 

        # create the new Task and append it to the Pipeline
        new_view = View(
            user_function, 
            args, 
            kwargs, 
            self,
            root_only)
        self.Views.append(new_view)

        if self.isRoot(): log(f"[Added View] {new_view.getString()}")


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
            if pipe.isRoot(): log(f"Initializing parallel run with {pipe.getCommSize()} processes")

        if rerun:
            pipe.clearCache()

        if memcheck:
            tracemalloc.start(25) # TODO: Move this to .env

        pipe.comm.Barrier()

        run_this_iteration = []
        waiting = [task for task in pipe.Tasks if not task.done]

        num_waiting = len(waiting)

        if pipe.isRoot(): log(f"---\n Starting a run with {num_waiting} tasks.\n---\n")

        MAX_ITERATIONS = 10000
        iterations = 0
        while num_waiting > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [task for task in waiting if task.readyToRun()]

            for i, task in enumerate(run_this_iteration):

                if parallel:
                    # round robin distribute Tasks to processes
                    if i % pipe.getCommSize() == pipe.getCommRank():
                        log(f"[Rank {pipe.getCommRank()}] running: " + task.getString())

                        try:
                            task.run()
                        except Exception as e:
                            ex_type, ex_value, ex_traceback = sys.exc_info()
                            error(ex_type.__name__ +' '+ str(ex_value), 
                                  fatal=False,
                                  task=task
                            )
                            # TODO: Broadcast that this Task has failed to other processes

                    else:
                        # Mark this Task done on other processes
                        # TODO: Gather Task successes and failures at the
                        # current Barrier step
                        task.done = True
                else:
                    task.run()

            pipe.comm.Barrier()

            waiting = [task for task in pipe.Tasks if not task.done]

            log(f"[Rank {pipe.getCommRank()}] waiting on {len(waiting)} Tasks")

            if pipe.isRoot(): log(f"---\nIteration {iterations} finished. {len(waiting)} Tasks left\n---")
            if len(waiting) != 0 and num_waiting <= len(waiting):
                error("Looks like the last run didn't complete any Tasks. Check your script for missing dependencies. Exiting")

            num_waiting = len(waiting)
            
        # end main while loop

        if pipe.isRoot(): log(f"Finished all tasks after {iterations} iterations")

        run_this_iteration = []
        waiting = [view for view in pipe.Views]

        if pipe.isRoot(): log(f"---\n {len(waiting)} views remaining.\n---\n")

        while len(waiting) > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [view for view in waiting if view.readyToRun()]

            for i, view in enumerate(run_this_iteration):

                if view.root_only:
                    if pipe.isRoot(): 
                        log(f"[Rank {pipe.getCommRank()}] running: " + view.getString())
                        view.run()
                    else:
                        view.shown = True
                elif i % pipe.getCommSize() == pipe.getCommRank():
                    log(f"[Rank {pipe.getCommRank()}] running: " + view.getString())
                    view.run()
                else:
                    view.shown = True
                # end if
            # end for view 

            pipe.comm.Barrier()

            waiting = [view for view in pipe.Views if not view.shown]
        # end while

        if pipe.isRoot(): log(f"---\n Completed all views .\n---\n")

        # end Views while loop

        # TODO: Fix this so it works in parallel
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

        if pipe.isRoot(): log("All done.")
          

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

        
