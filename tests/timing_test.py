
from ndustria import AddTask, AddView, Pipeline

import time, random

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

Pipeline.clearCache()


if __name__ == "__main__":
    for i in range(5):

        three_second_function()
        arbitrary_delay_function()
        time_test()

    Pipeline.run(timeit=True)