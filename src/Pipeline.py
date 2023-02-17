# Singleton class that contains the entire analysis pipeline
from Task import Task
from Cache import Cache
from View import View

import os
from Logger import log, error

from mpi4py import MPI

class Pipeline:

    Tasks = []
    Views = []

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Pipeline, cls).__new__(cls)
            
            cls.instance.cache = Cache("./temp")
            log(f"Created new Pipeline with cache located at {cls.instance.cache.path}")

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
            root_proc_only)
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
    def run(rerun=False, parallel=False):

        pipe = Pipeline()

        if parallel:
            log(f"Initializing parallel run with {pipe.getCommSize()} processes")

        if rerun:
            pipe.clearCache()

        pipe.comm.Barrier()

        run_this_iteration = []
        waiting = [task for task in pipe.Tasks if not task.done]

        num_waiting = len(waiting)

        MAX_ITERATIONS = 10000
        iterations = 0
        while num_waiting > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [task for task in waiting if task.readyToRun()]


            #TODO: May want to put a try catch around this to 
            # help it fail gracefully
            for i, task in enumerate(run_this_iteration):

                if parallel:
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
            pipe.cache.clear()
            
        for task in pipe.Tasks:
            task.done = False
            task.result = None

    