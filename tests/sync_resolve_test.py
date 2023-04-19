from ndustria import AddTask, AddView, Pipeline
from ndustria.src.Task import TaskNotReadyError
import numpy as np


@AddTask()
def create_many_random_arrays(N=10, size=10):

    arrays = np.zeros((N, size))
    for i in range(N):
        arrays[i] = np.random.random(size=size)
    return arrays

@AddTask()
def create_array_likes(old_arrays):

    new_arrays = np.zeros_like(old_arrays)
    for i, arr in enumerate(old_arrays):
        new_arrays[i] = np.random.random(len(arr))
    return new_arrays

@AddTask()
def do_analysis(data):

    result = {
        "sum" : np.sum(data),
        "mean" : np.mean(data),
        "std" : np.std(data),
        "length" : len(data)
    }
    return result


@AddView()
def view_data(data):
    print(f"""
Viewing data for {data['length']} random numbers: 
        Sum     : {data['sum']}
        Mean    : {data['mean']}
        Std dev : {data['std']}
""")


for i in range(2, 5):
    size = 500


    # Create a setup that will fail with the TaskNotReadError
    # This will only fail successfully if a result for create_many_random_arrays
    # does not exist in cache. 
    # 
    # 
    try:
        random_arrays = create_many_random_arrays(i, size)
        arrays_like = create_array_likes(random_arrays)

        for i in arrays_like:
            other_analysis = do_analysis(i)
    except TaskNotReadyError as e:
        print("Task failed successfully!")


    
    try:
        # Now run the dependency that caused us to fail
        random_arrays = create_many_random_arrays()
        random_arrays.run()

        # and try it again
        arrays_like = create_array_likes(random_arrays)

        for i in arrays_like:
            other_analysis = do_analysis(i)
            view_data(other_analysis)
    except TaskNotReadyError as e:
        print("Task failed unsuccessfully <:'C")



Pipeline.run(rerun=True)