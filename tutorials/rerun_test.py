from ndustria import Pipeline
import numpy as np

pipe = Pipeline()

@pipe.AddFunction()
def matrix_multiplication(N=10):
    matrix_1 = np.random.rand(N, N)
    matrix_2 = np.random.rand(N, N)
    result_matrix = np.matmul(matrix_1, matrix_2)
    return result_matrix

@pipe.AddFunction(rerun = True)                     ### ONLY CHANGE IS HERE ON THIS LINE. WE HAVE SET rerun=True ###
def matrix_parameters(matrix):

    num_rows, num_cols = matrix.shape
    
    print("\n----------------------------------------")
    print("Datatype of Matrix Object:", type(matrix))
    print("Size of Matrix:", matrix.shape)
    print("Total Number of Elements:", num_rows*num_cols)
    print("----------------------------------------\n")

    return 0
          
def main(): 
    for i in range(10, 15):
        random_arrays = matrix_multiplication(N=2**i)
        matrix_parameters(random_arrays)

    pipe.run()

main()