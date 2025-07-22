from ndustria import Pipeline
import numpy as np

pipe = Pipeline()

@pipe.AddFunction()
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

@pipe.AddFunction()
def do_sum(data):

    result = {
        "sum" : np.sum(data),
        "length" : len(data)
    }
    return result

@pipe.AddFunction()
def do_mean(data):

    result = {
        "mean" : np.mean(data),
        "length" : len(data)
    }
    return result

@pipe.AddFunction()
def do_std(data):

    result = {
        "std" : np.std(data),
        "length" : len(data)
    }
    return result

@pipe.AddFunction()
def view_data(sum_data, mean_data, std_data):

    print(f"""
Viewing data for {sum_data['length']} random numbers: 
        Sum     : {sum_data['sum']}
        Mean    : {mean_data['mean']}
        Std dev : {std_data['std']}
""")
          
    return "no_result"



for i in range(5, 8):
    N = 10**i
    random_array = create_random_array(N=N)
    a = do_sum(random_array)
    b = do_mean(random_array)
    c = do_std(random_array)
    view_data(a,b,c)


pipe.run(run_all=True)
