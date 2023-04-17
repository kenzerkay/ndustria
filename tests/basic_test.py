from ndustria import AddTask, AddView, Pipeline
import numpy as np


@AddTask()
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

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


for i in range(5, 8):
    N = 10**i
    random_arrays = create_random_array(N=N)
    analysis = do_analysis(random_arrays)
    view_data(analysis)


Pipeline.run(rerun=True, memcheck=True)
