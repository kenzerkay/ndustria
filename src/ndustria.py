import functools
from Pipeline import Pipeline

def AddTask(
    depends_on=None,
    match="most_recent"
):

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