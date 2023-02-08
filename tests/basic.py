from ndustria import AddTask, AddView, Pipeline
import numpy as np


@AddTask()
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

@AddTask(depends_on=create_random_array)
def do_analysis(data):

    result = {
        "sum" : np.sum(data),
        "mean" : np.mean(data),
        "std" : np.std(data),
        "length" : len(data)
    }
    return result

@AddView(views=do_analysis)
def view_data(data):
    print(f"""
Viewing data for {data['length']} random numbers: 
        Sum     : {data['sum']}
        Mean    : {data['mean']}
        Std dev : {data['std']}
""")


for i in range(5, 8):
    N = 10**i
    create_random_array(N=N)
    do_analysis()
    view_data()


Pipeline.run()
Pipeline.printCacheInfo()
Pipeline.printLog()