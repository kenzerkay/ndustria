"""Defines the decorators that convert user defined functions into Tasks and Views.

"""

import functools
from .Pipeline import Pipeline

def AddTask(
    depends_on=None,
    match="most_recent"
):
    """When the decorated function is called, it creates a Task and adds it the Pipeline instead of running. 

    Any positional or keyword arguments passed to the function are saved and passed along to the user function
    at execution time. 

    Arguments:
    depends_on -- A function, or list of functions whose Tasks must be run before this Task can be executed
    match -- a string that tells ndustria how to assign dependencies. 
             The options are:
             most_recent -- Finds a Task that matches the given function name that was most recently added to the Pipeline
             all -- Finds all Tasks that match the given function name

    -----------------------------------------------------------------------------------------
    IMPORTANT Part I
    -----------------------------------------------------------------------------------------

    If this Task depends on a previously run Task, then the first positional argument in the function definition
    MUST represent the return value of that Task (if there are multiple dependencies, see the documentation of Task.getDependencyData
    for more information)

    When a decorated, dependent function is called, do NOT pass in that first positional argument. Ndustria will take care of that.
    Just pass along any extraneous arguments required to run your function.

    Example:

    # When defining the function
    @AddTask(depends_on=independent_function)
    def dependent_function(result_from_independent_function, a, b, c):
        
        d = do_something_with( result_from_independent_function )

        result = a + b * c - d

        return result

    # When setting up the pipeline

    # run independent_function first
    other_function()

    # then call dependent_function() WITHOUT first argument
    dependent_function(a, b, c)

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

            pipe.addTask(
                user_function, 
                args, 
                kwargs, 
                depends_on=depends_on,
                match=match
            )

        return inner_wrapper
    return outer_wrapper

    
def AddView(
    looks_at=None,
    match="most_recent",
    root_proc_only=False
):
    """The decorated function is converted in a View and added to the Pipeline.

    Views are intended to produce a human-interpretable view of data created by a Task.

    Views function similarly to Tasks by differ in a few important ways. 

    Firstly, Views are always executed and their results are not saved to the cache. Avoid putting long-running code
    into Views; that's what Tasks are for. 

    Secondly, Views are always dependent on a Task. There is no such thing as an independent View.

    The decorated function must have its first positional argument accept the result of its dependent Tasks, same as 
    any dependent Task. When calling the decorated function as part of regular Pipeline setup, leave out the first
    positional argument, again, the same as any dependent Task.

    Views can take positional or keyword arguments in the same way as Tasks, and they use the same matching strategies 
    to determine what their dependencies are. 

    Views can also be run soley on the root process (rank=0) if running in parallel.

    Arguments:
    looks_at -- a function, or list of functions whose Task results are represented by this View
    match -- a string that tells ndustria how to assign dependencies. 
             The options are:
             most_recent -- Finds a Task that matches the given function name that was most recently added to the Pipeline
             all -- Finds all Tasks that match the given function name
    root_proc_only -- Runs this View only on the root process (rank = 0)
    """
    pipe = Pipeline()

    def outer_wrapper(user_function):
        @functools.wraps(user_function)
        def inner_wrapper(*args, **kwargs):

            pipe.addView(
                user_function, 
                args, 
                kwargs, 
                looks_at,
                match=match,
                root_proc_only=root_proc_only
            )

        return inner_wrapper
    return outer_wrapper