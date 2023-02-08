import functools
from Pipeline import Pipeline

def AddTask(
    depends_on=None
):

    pipe = Pipeline()

    def outer_wrapper(user_function):
        @functools.wraps(user_function)
        def inner_wrapper(*args, **kwargs):

            pipe.addTask(
                user_function, 
                args, 
                kwargs, 
                depends_on=depends_on
            )

        return inner_wrapper
    return outer_wrapper

    
def AddView(
    views=None
):

    pipe = Pipeline()

    def outer_wrapper(user_function):
        @functools.wraps(user_function)
        def inner_wrapper(*args, **kwargs):

            pipe.addView(
                user_function, 
                args, 
                kwargs, 
                views
            )

        return inner_wrapper
    return outer_wrapper