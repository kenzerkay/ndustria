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

@AddView(views=[do_sum, do_mean, do_std])
def view_data(dataDict):

    sum_data = dataDict["do_sum"]
    mean_data = dataDict["do_mean"]
    std_data = dataDict["do_std"]

    print(f"""
Viewing data for {sum_data['length']} random numbers: 
        Sum     : {sum_data['sum']}
        Mean    : {mean_data['mean']}
        Std dev : {std_data['std']}
""")

Pipeline.clearCache()

for i in range(5, 8):
    N = 10**i
    create_random_array(N=N)
    do_sum()
    do_mean()
    do_std()
    view_data()


Pipeline.run()
Pipeline.printCacheInfo()
Pipeline.printLog()