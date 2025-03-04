from ndustria import Pipeline
import numpy as np

pipe = Pipeline(timeit=True, memcheck=True)

rr = True

@pipe.AddFunction(rerun = rr)
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

@pipe.AddFunction(rerun = rr)
def do_analysis(data):

    result = {
        "sum" : np.sum(data),
        "mean" : np.mean(data),
        "std" : np.std(data),
        "length" : len(data)
    }
    return result

@pipe.AddFunction(rerun=True)
def view_data(data, out_file="data.txt"):

    f = open(out_file, "w")

    data_string = f"""
Viewing data for {data['length']} random numbers: 
        Sum     : {data['sum']}
        Mean    : {data['mean']}
        Std dev : {data['std']}
"""

    print(data_string)

    print(data_string, file=f)
          
    f.close()
    return out_file
          

for i in range(5, 8):
    N = 10**i
    random_arrays = create_random_array(N=N)
    analysis = do_analysis(random_arrays)
    view_data(analysis, out_file=f"data_N_{N}.txt")

# This one should skip the array creation and analysis
# and just print the results of the last run
pipe.run()
