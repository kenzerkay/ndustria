from ndustria import Pipeline
import numpy as np

pipe = Pipeline()

@pipe.AddFunction()
def create_random_array(N=10):
    arr = np.random.rand(N)
    return arr

@pipe.AddFunction()
def do_analysis(data):
    result = {
        "sum" : np.sum(data),
        "mean" : np.mean(data),
        "std" : np.std(data),
        "length" : len(data)
    }
    return result

@pipe.AddFunction()
def view_data(data, out_file="data.txt"):

    # Create a string that holds our analysis information 
    data_string = f"""
        Viewing data for {data['length']} random numbers: 
        Sum     : {data['sum']}
        Mean    : {data['mean']}
        Std dev : {data['std']}
    """

    # Print results to terminal
    print(data_string)

    # Save results to file 
    f = open(out_file, "a")
    f.write(data_string) 
    f.close()
    return out_file
          
def main(): 
    for i in range(3, 8):
        random_arrays = create_random_array(N=10**i)
        analysis = do_analysis(random_arrays)
        view_data(analysis, out_file=f"basic_test.txt" )

    pipe.run()

main()