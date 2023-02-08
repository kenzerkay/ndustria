# Singleton class that contains the entire analysis pipeline
from Task import Task
from Cache import Cache
from View import View

from log import debug, error

class Pipeline:

    Tasks = []
    Views = []

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Pipeline, cls).__new__(cls)
            
            cls.instance.cache = Cache("./temp")
            debug(f"Created new Pipeline with cache located at {cls.instance.cache.path}")

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
        old_task = None
        if depends_on != None:
            old_task = self.MostRecentMatching(depends_on)
            

        # create the new Task and append it to the Pipeline
        new_task = Task(
            len(self.Tasks),
            user_function, 
            args, 
            kwargs, 
            self,
            depends_on=old_task)
        self.Tasks.append(new_task)

        if new_task.done:
            debug(f"Task {new_task} will be skipped")
        else:
            debug(f"Added new Task: {new_task}")

    def addView(self, 
        user_function, 
        args, 
        kwargs, 
        task_name
    ):
        """Factory function for creating all new Views"""
        # figure out which Task this View should show
        old_task = self.MostRecentMatching(task_name)
            

        # create the new Task and append it to the Pipeline
        new_view = View(
            user_function, 
            args, 
            kwargs, 
            self,
            old_task)
        self.Views.append(new_view)

        debug(f"Added new View: {new_view}")


    # First and default Dependency strategy
    # Find the most recently added task that matches the depends_on function
    def MostRecentMatching(self, depends_on):

        # loop over the list in reverse since we want the 
        # most recently added 
        old_task = None
        for i in range(len(self.Tasks)-1, -1, -1):
            old_task = self.Tasks[i]
            if old_task.user_function.__name__ == depends_on.__name__:
                return old_task

        error(f"No matching Task found for '{depends_on.__name__}'")

        

    @staticmethod
    def run():

        pipe = Pipeline()

        run_this_iteration = []
        waiting = [task for task in pipe.Tasks if not task.done]

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



        debug(f"Finished all tasks after {iterations} iterations")

        run_this_iteration = []
        waiting = [view for view in pipe.Views]

        while len(waiting) > 0 and iterations < MAX_ITERATIONS:
            iterations+=1

            run_this_iteration = [view for view in waiting if view.readyToRun()]

            for view in run_this_iteration:
                view.run()

            waiting = [view for view in pipe.Views if not view.shown]

        print("All done.")
          