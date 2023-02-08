from ndustria import AddTask, AddView, Pipeline
import numpy as np


@AddTask()
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

@AddTask(depends_on=create_random_array)
def do_sum(data):

    result = {
        "sum" : np.sum(data),
        "length" : len(data)
    }
    return result

@AddTask(depends_on=create_random_array)
def do_mean(data):

    result = {
        "mean" : np.mean(data),
        "length" : len(data)
    }
    return result

@AddTask(depends_on=create_random_array)
def do_std(data):

    result = {
        "std" : np.std(data),
        "length" : len(data)
    }
    return result

@AddView(task=do_sum)
def view_data(data):
    print(f"""
Viewing data for {data['length']} random numbers: 
        Sum     : {data['sum']}
""")


for i in range(5, 8):
    N = 10**i
    create_random_array(N=N)
    do_sum()
    view_data()

Pipeline.clearCache()
Pipeline.run()
Pipeline.printCacheInfo()
Pipeline.printLog()