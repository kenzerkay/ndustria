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
    size = 1000

    random_arrays = create_many_random_arrays(N=i, size=size)
    # here, 'random_arrays' is a an unresolved Task with no idea how many 
    # arrays it will eventually create once it resolves

    # in order for this to be processed correctly
    # random_arrays has to resolve as its being asked to iterate
    #
    # i.e. random_arrays runs as this for loop is being setup
    # if random_arrays is not readyToRun, this should error
    for arr in random_arrays:
        analysis = do_analysis(arr)
        view_data(analysis)


    try:
        random_arrays = create_many_random_arrays()
        fake_analysis = do_analysis(random_arrays)

        for i in fake_analysis:
            other_analysis = do_analysis(i)
    except TaskNotReadyError as e:
        print("Test passed!")


Pipeline.run(rerun=True)