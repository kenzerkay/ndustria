"""Defines the decorators that convert user defined functions into Tasks and Views.

"""

import functools
from .Pipeline import Pipeline

def AddTask(
    rerun=False
):
    """When the decorated function is called, it creates a Task and adds it the Pipeline instead of running. 

    Any positional or keyword arguments passed to the function are saved and passed along to the user function
    at execution time. 

    Arguments:
        rerun -- boolean. If True, always runs this Task, regardless of what's in the cache
                 Use this when debugging your pipeline.
                 Default value: False
    -----------------------------------------------------------------------------------------
    IMPORTANT Part I
    -----------------------------------------------------------------------------------------


    Example Usage:

    # When defining the function
    @AddTask()
    def dependent_function(result_from_independent_function, a, b, c):
        
        d = do_something_with( result_from_independent_function )

        result = a + b * c - d

        return result

    # When setting up the pipeline

    # run independent_function first
    result_from_independent_function = other_function()

    # then call dependent_function() first argument
    dependent_function(result_from_independent_function, a, b, c)

    # The actual execution of both functions occurs here
    Pipeline.run()
        
    -----------------------------------------------------------------------------------------

    -----------------------------------------------------------------------------------------
    IMPORTANT Part II
    -----------------------------------------------------------------------------------------

    Any arguments passed to a Task's user_function must have a unique string representation
    given by the str() function. This is due to the default behavior of the str() function
    for classes in Python, which returns the memory address of the object. That address is almost
    always unique to a specific run of the code and therefore produces indeterministic hashes
    even though the underlying data might be exactly the same.
    -----------------------------------------------------------------------------------------

    
    """

    pipe = Pipeline()

    def outer_wrapper(user_function):
        @functools.wraps(user_function)
        def inner_wrapper(*args, **kwargs):

            return pipe.addTask(
                user_function, 
                args, 
                kwargs,
                rerun=rerun
            )

        return inner_wrapper
    return outer_wrapper

    
def AddView(
    root_only=False
):
    """The decorated function is converted in a View and added to the Pipeline.

    Views are intended to produce a human-interpretable view of data created by a Task.

    Views function similarly to Tasks by differ in a few important ways. 

    Firstly, Views are always executed and their results are not saved to the cache. Avoid putting long-running code
    into Views; that's what Tasks are for. 

    Secondly, Views are always dependent on a Task. There is no such thing as an independent View.

    Views can take positional or keyword arguments in the same way as Tasks and they
    will auto-resolve any Tasks passed in as an argument to determine what they depend on.

    Views can also be run soley on the root process (rank=0) if running in parallel.

    Arguments:
    root_only -- Runs this View only on the root process (rank = 0)
    """
    pipe = Pipeline()

    def outer_wrapper(user_function):
        @functools.wraps(user_function)
        def inner_wrapper(*args, **kwargs):

            pipe.addView(
                user_function, 
                args, 
                kwargs,
                root_only=root_only
            )

        return inner_wrapper
    return outer_wrapper