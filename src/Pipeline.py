# Singleton class that contains the entire analysis pipeline
from Task import Task
from Cache import Cache
from View import View

import os, sys
from Logger import log, error

from mpi4py import MPI

class Pipeline:

    Tasks = []
    Views = []

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
        pass

    def addTask(self, 
        user_function, 
        args, 
        kwargs, 
        depends_on=None,
        match="most_recent"
    ):
        """Factory function for creating all new Tasks"""
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
        """Factory function for creating all new Views"""

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
        return self.comm.Get_rank()

    def getCommSize(self):
        return self.comm.Get_size()
    
    def isRoot(self):
        return self.comm.Get_rank() == 0

    """
    The main Task running function
    """
    @staticmethod
    def run(
        rerun=False, 
        parallel=False,
        dryrun=False,
        timeit=False ):

        pipe = Pipeline()

        pipe.timeit = timeit
        pipe.dryrun = dryrun

        if parallel:
            log(f"Initializing parallel run with {pipe.getCommSize()} processes")

        if rerun:
            pipe.clearCache()

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

            

        log("All done.")
          

    @staticmethod
    def printCacheInfo():
        pipe = Pipeline()

        os.system(f"cat {pipe.cache.info_file}")

    @staticmethod
    def printLog():
        pipe = Pipeline()

        os.system(f"cat {pipe.cache.log_file}")

    @staticmethod
    def clearCache():
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

    """Misc utility functions"""
    def getAllHashCodes(self):
        return [task.getHashCode() for task in self.Tasks]
        