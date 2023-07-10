"""This is a test of the packaging function. What this does is gather together 
all cache files produced as part of this pipeline and zip them up for distribution
to another computer system. Useful for performing mining on a super computer and
then looking at the data locally on a laptop.
"""

import numpy as np

from ndustria import AddTask, Pipeline

pipe = Pipeline(name="package_test", rerun=True)

@pipe.AddTask()
def create_random_array(seed=100):

    np.random.seed(100)
    arr = np.random.rand(1000)

    return arr

for i in range(10):
    create_random_array(i)

pipe.run()

pipe.pack(save_to="package_test.tar.gz")
