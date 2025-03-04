# from ndustria import AddTask, AddView, Pipeline
# import numpy as np

# @AddTask()
# def allocate_mem(N):
#     """Allocates N kB of zeros"""

#     # 32 bits is 4 bytes
#     # array of size N is N*4 bytes
#     # so we want to allocate N/4 * 1000
#     block = np.zeros(int(N*1000/4), dtype="float32")

#     return block


# @AddTask()
# def test_peak_mem(N):
#     """Allocates 2*N kB of zeros, then deletes that and returns N kB instead"""

#     block = np.zeros(int(2*N*1000/4), dtype="float32")
#     block = np.zeros(int(N*1000/4), dtype="float32")

#     return block


# if __name__ == "__main__":

#     for i in range(1,4):
#         allocate_mem(i*1000)
#         test_peak_mem(i*1000)



# Pipeline.run(rerun=True, memcheck=True)