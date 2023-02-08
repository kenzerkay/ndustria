# Singleton class that contains the entire analysis pipeline
from Task import Task
from Cache import Cache
from View import View

import os
from Logger import log, error, setLogFile

class Pipeline:

    Tasks = []
    Views = []

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Pipeline, cls).__new__(cls)
            
            cls.instance.cache = Cache("./temp")
            cls.instance.log_file = os.path.join(cls.instance.cache.path, "pipe.out")
            setLogFile(cls.instance.log_file)
            log(f"Created new Pipeline with cache located at {cls.instance.cache.path}")

        return cls.instance

    def __init__(self):
        pass

    def addTask(self, 
        user_function, 
        args, 
        kwargs, 
        depends_on=None
    ):
        """Factory function for creating all new Tasks"""
        # figure out which Task this Task should depend on, if any
        dependencies = None
        if depends_on != None:
            dependencies = self.MostRecentMatching(depends_on)
            

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
        views
    ):
        """Factory function for creating all new Views"""

        # figure out which Task this View should show
        old_task = self.MostRecentMatching(views)
            

        # create the new Task and append it to the Pipeline
        new_view = View(
            user_function, 
            args, 
            kwargs, 
            self,
            old_task)
        self.Views.append(new_view)

        log(f"Added new View: {new_view}")


    # First and default Dependency strategy
    # Find the most recently added task that matches the depends_on function
    def MostRecentMatching(self, depends_on):

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

    @staticmethod
    def run():

        pipe = Pipeline()

        run_this_iteration = []
        waiting = [task for task in pipe.Tasks if not task.done]

        num_waiting = len(waiting)

        MAX_ITERATIONS = 10000
        iterations = 0
        while len(waiting) > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [task for task in waiting if task.readyToRun()]


            #TODO: May want to put a try catch around this to 
            # help it fail gracefully
            for task in run_this_iteration:
                task.run()

            waiting = [task for task in pipe.Tasks if not task.done]

            log(f"Iteration {iterations} finished. {len(waiting)} Tasks left")
            if len(waiting) != 0 and num_waiting <= len(waiting):
                error("Looks like the last run didn't complete any Tasks. Check your script for missing dependencies. Exiting")

            num_waiting = len(waiting)
            




        log(f"Finished all tasks after {iterations} iterations")

        run_this_iteration = []
        waiting = [view for view in pipe.Views]

        while len(waiting) > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [view for view in waiting if view.readyToRun()]

            for view in run_this_iteration:
                view.run()

            waiting = [view for view in pipe.Views if not view.shown]

        log("All done.")
          

    @staticmethod
    def printCacheInfo():
        pipe = Pipeline()

        os.system(f"cat {pipe.cache.info_file}")

    @staticmethod
    def printLog():
        pipe = Pipeline()

        os.system(f"cat {pipe.log_file}")

    @staticmethod
    def clearCache():
        pipe = Pipeline()
        pipe.cache.clear()

    