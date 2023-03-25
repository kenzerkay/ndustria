
from ndustria import AddTask, AddView, Pipeline

import time, random

###############################################################
# IMPORTANT
# This test exposes an important disadvantage of ndustria, 
# which is that it relies on deterministic user functions. 
# The three_second_function returns a random
# number every time it executes, meaning that the function
# does not return a unique result for a unique input.
# This results in the saved cache files being unreliable as
# each one will be overwritten with different values. 
# Running with rerun=True will still work, but re-running 
# from cache will not. 
# 
# This doesn't really affect this test since the whole point
# is to test the timing functionality, but be warned that 
# non-deterministic functions do not play nice with ndustria
###############################################################
@AddTask()
def three_second_function():

    print("Waiting for 3 seconds")
    time.sleep(3)

    # the next function will run for a random amount of time
    rand_int = random.randint(0, 3)

    return rand_int

@AddTask(depends_on=three_second_function)
def arbitrary_delay_function(number):

    time.sleep(number)

    return number

@AddView(looks_at=arbitrary_delay_function)
def time_test(number):

    # we'll know how long it ran when the view runs
    print(f"The arbitrary function ran for {number} seconds")

    # check the timing data to see if its correct


if __name__ == "__main__":
    three_second_function()
    arbitrary_delay_function()
    time_test()

    Pipeline.run(rerun=True)