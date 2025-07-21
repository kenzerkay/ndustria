# Industrialize the process of data mining with ndustria! 

# Main features (WIP)

* Simple multiprocess parallelization of arbitrary python code
* Dynamic load balancing
* Caching and reuse of intermediate data products
* Association of code and parameters with data, which updates whenever code updates
* Ease of use and understanding. Designed to work with minimal modification to existing code. 

ndustria is being built as part of a PhD dissertation and is in active development.

# Installation

## Simple Use Installation  

*** This is still a WIP. I have not posted it publicly yet ***  

```
pip install ndustria 
```

```
ndustria-init
```

```
cd
source .bashrc
```

Check that you can import and use ndustria
```
python -c "from ndustria import Pipeline"
```

## Development Installation 
Clone Repository from github and enter into the directory
```
git clone <YOUR_VERSION_URL>
cd ndustria
```

Pip install the package and run the set up bash script
```
pip install -e .
ndustria-init
```
`ndustria-init` creates a config file for ndustria and sets up your "cache" directory where the returns of your functions will be saved. If at any point you want to change your cache directory you must rerun `ndustria-init`. This script also adds a couple of things to path in your shell start up file. Therefore in order use ndustria you must reinitialize shell. If you have bash you can complete this with:
```
cd
source .bashrc
```
If you use a different shell you must use whatever command corresponds to your shell. If you are running an environment (like conda) you may need to reinitialize it after you run `source .bashrc`

Check that you can import and use ndustria
```
python -c "from ndustria import Pipeline"
```

# How ndustria works

ndustria works by creating a `Pipeline()` or list of tasks. Each independent task should be separated into a different function and we can use the `*.AddFunction()` decorator to add this task to the Pipeline. The information in the return statement will be saved to disk and can be used by other task both during this run of the script and for other runs. For Example:

```
from ndustria import Pipeline               # <-- 1. Import Pipeline from ndustria 

pipe = Pipeline()                           # <-- 2. Create a new Pipeline
  
@pipe.AddFunction()                         # <-- 3. Use decorator to add function to Pipeline
def filterLargeTask(path, filter_params):   # <-- 4. Function that surrounds your long-running task

    # this takes a long time to run, 
    small_subset = filterLargeDataset(path, filter_params)

    return small_subset                     #<-- 5. Save your work to disk by adding it to the return statement
```

Once you have added one or more functions to your `Pipeline()` you can use `pipe.run()` to execute your whole Pipeline. To see this in action please follow the **Tutorial**



# Tutorials

This tutorial will walk you through a set of test scripts so that you can explore the functionality of ndustria. `cd` into tests to access the test scripts 
```
cd tests
```

## Simple Example

We can start by running: 

```
python simple_example.py
```

We can breakdown the terminal outputs to better understand how ndustria woks. The first thing ndustria does is define all of the "tasks" in your Pipeline. 

```
[Added Task] matrix_multiplication(N=1024)
[Added Task] matrix_parameters(matrix_multiplication(N=1024))
[Added Task] matrix_multiplication(N=2048)
[Added Task] matrix_parameters(matrix_multiplication(N=2048))
[Added Task] matrix_multiplication(N=4096)
[Added Task] matrix_parameters(matrix_multiplication(N=4096))
[Added Task] matrix_multiplication(N=8192)
[Added Task] matrix_parameters(matrix_multiplication(N=8192))
[Added Task] matrix_multiplication(N=16384)
[Added Task] matrix_parameters(matrix_multiplication(N=16384))
---
 Starting a run with 10 tasks.
---
```

ndustria is able to detect which tasks depend on other tasks and will run independent tasks first and then continue onto other tasks that require the results of previous tasks. For example we can see that ndustria performs all of the `matrix_multiplication` tasks first and then `matrix_parameters` tasks even though this is not the order they would be executed in without ndustria. We can see this because ndustria saves the results to all 5 `matrix_multiplication` tasks before moving onto any of the `matrix_parameters` tasks. This behavior is implemented for better resource utilization in parallel programs which will be discussed later. 

```
Saved result of matrix_multiplication(N=1024) to /home/kenzerkay/.ndustria_cache/56c6f56aa673d1699f3e6a8bfe12410f
Saved result of matrix_multiplication(N=2048) to /home/kenzerkay/.ndustria_cache/d47090ac5456be3b9d88338994375e93
Saved result of matrix_multiplication(N=4096) to /home/kenzerkay/.ndustria_cache/48f31c91db8a3343f81b18d15f3e5ac6
Saved result of matrix_multiplication(N=8192) to /home/kenzerkay/.ndustria_cache/4538eba62c2ed6d4bf9a01c44669bf3a
Saved result of matrix_multiplication(N=16384) to /home/kenzerkay/.ndustria_cache/0045520ec0f36d5affa5248d172a721e
[Rank 0] waiting on 5 Tasks
---
Iteration 1 finished. 5 Tasks left
---
```

Then ndustria runs all 5 `matrix_parameters` tasks and output the some simple parameters to the terminal as we ask it to do in the function. 


```
----------------------------------------
Datatype of Matrix Object: <class 'numpy.ndarray'>
Size of Matrix: (1024, 1024)
Total Number of Elements: 1048576
----------------------------------------

Saved result of matrix_parameters(matrix_multiplication(N=1024)) to /home/kenzerkay/.ndustria_cache/98f5d9aeb878357950c9975a54cf0c3d

----------------------------------------
Datatype of Matrix Object: <class 'numpy.ndarray'>
Size of Matrix: (2048, 2048)
Total Number of Elements: 4194304
----------------------------------------

Saved result of matrix_parameters(matrix_multiplication(N=2048)) to /home/kenzerkay/.ndustria_cache/a99d386c5432b7a960413e92f58b1cd3

----------------------------------------
Datatype of Matrix Object: <class 'numpy.ndarray'>
Size of Matrix: (4096, 4096)
Total Number of Elements: 16777216
----------------------------------------

Saved result of matrix_parameters(matrix_multiplication(N=4096)) to /home/kenzerkay/.ndustria_cache/77b869459b15a893a39ef61482f61702

----------------------------------------
Datatype of Matrix Object: <class 'numpy.ndarray'>
Size of Matrix: (8192, 8192)
Total Number of Elements: 67108864
----------------------------------------

Saved result of matrix_parameters(matrix_multiplication(N=8192)) to /home/kenzerkay/.ndustria_cache/4abfc0e48abadabb16d4f57974d53752

----------------------------------------
Datatype of Matrix Object: <class 'numpy.ndarray'>
Size of Matrix: (16384, 16384)
Total Number of Elements: 268435456
----------------------------------------

Saved result of matrix_parameters(matrix_multiplication(N=16384)) to /home/kenzerkay/.ndustria_cache/cb02ea6be2f7c56497cffca9c526e225
[Rank 0] waiting on 0 Tasks
---
Iteration 2 finished. 0 Tasks left
---
```

If all tasks in your program run successfully you should see the "All done" message at the end. 

```
Finished all tasks after 2 iterations
All done.
```

If you run `python simple_example.py` again you should see 
```
[Cache hit!] matrix_multiplication(N=1024) can be skipped
[Cache hit!] matrix_parameters(matrix_multiplication(N=1024)) can be skipped
[Cache hit!] matrix_multiplication(N=2048) can be skipped
[Cache hit!] matrix_parameters(matrix_multiplication(N=2048)) can be skipped
[Cache hit!] matrix_multiplication(N=4096) can be skipped
[Cache hit!] matrix_parameters(matrix_multiplication(N=4096)) can be skipped
[Cache hit!] matrix_multiplication(N=8192) can be skipped
[Cache hit!] matrix_parameters(matrix_multiplication(N=8192)) can be skipped
[Cache hit!] matrix_multiplication(N=16384) can be skipped
[Cache hit!] matrix_parameters(matrix_multiplication(N=16384)) can be skipped
---
 Starting a run with 0 tasks.
---

Finished all tasks after 0 iterations
All done.
```

This means that ndustria "recognizes" that we have run all of these scripts before and it would pull the data from disk if needed. Since all tasks have been run before ndustria does not execute any tasks.

## Rerun Parameter

As we have seen from the last example ndustria *does not* rerun tasks that it has already run, in fact this is what it is designed to do. However, what if we wanted to force it to rerun a task regardless of whether or not we change the code. This can be done using the `rerun=True` parameter within the decorator (by default `rerun=False`). We can see this in `rerun_test.py` which is the same script as `simple_example.py` but the `matrix_parameters` function we have `rerun=True` in the decorator. When we run, 

```
python rerun_test.py
```

we can see the top of the output looks like 

```
[Cache hit!] matrix_multiplication(N=1024) can be skipped
[Added Task] matrix_parameters(matrix_multiplication(N=1024))
[Cache hit!] matrix_multiplication(N=2048) can be skipped
[Added Task] matrix_parameters(matrix_multiplication(N=2048))
[Cache hit!] matrix_multiplication(N=4096) can be skipped
[Added Task] matrix_parameters(matrix_multiplication(N=4096))
[Cache hit!] matrix_multiplication(N=8192) can be skipped
[Added Task] matrix_parameters(matrix_multiplication(N=8192))
[Cache hit!] matrix_multiplication(N=16384) can be skipped
[Added Task] matrix_parameters(matrix_multiplication(N=16384))
---
 Starting a run with 5 tasks.
---
```

Here ndustria runs the `matrix_parameters` tasks as instructed but does not run the `matrix_multiplication` tasks since we already have saved version of these functions and have not explicitly asked for them to be rerun. This can be helpful for debugging and recalling results from smaller functions that you may have printed to screen, etc. 


## Pipeline Keyword Arguments 

While `rerun` is the only keyword argument for individual decorators. ndustria `Pipelines` have a number of kwargs that can help you configure the run.
They are the following:

### Keyword arguments:
* name -- A name to give the pipeline for organizational purposes. If left blank, it will derive the name from the file used to run the code
* parallel -- If True, uses a round robin approach to assign Tasks to multiple processes and runs them in parallel
* dryrun -- If True, skips running Tasks but does everything else, including creating log files. Used to test complex pipelines
* timeit -- If True, keeps track of wallclock time of each Task. These data will be output to a csv file in the cache. Set to True by default due to low overhead
* memcheck -- If True, collects initial, peak, and final memory usage of each Task. These data will be output to a csv file in the cache. Can have high overhead if you allocate a lot of small objects
* profiling -- If True, executes line_profiling on all of the tasks in your Pipeline which gives timing results line by line for the function. 

By default all of these parameters are set to false, but we can test what happens if they are `True` with 

```
python pipeline_kwargs.py

```

<!-- Let's say you have a section of code that runs for a long time located at the top
of your analysis pipeline such that everything downstream of that code has to wait
for it. 

A common example would be filtering a large dataset for a small subset of data. 
```
# this takes a long time to run, 
small_subset = filterLargeDataset(path_to_dataset, filter_parameters)
```
ndustria implements a wrapper for long-running code that saves the result of that 
work and reuses and recycles it wherever it's needed. 

Here's what wrapping your code looks like
```
from ndustria import Pipeline # <-- 1. should be the only import you need

pipe = Pipeline() # <-- 2. Create a new Pipeline

@pipe.AddFunction() # <-- 3. function decorator adds functions to the pipeline
def filterLargeTask(path_to_dataset, filter_parameters): # <-- 4. wrapper function surrounds your long-running task

    # this takes a long time to run, 
    small_subset = filterLargeDataset(path_to_dataset, filter_parameters)

    return small_subset #<-- 5. save your work to disk by adding it to the return statement
```

First, wrap your long-running code in a python function and have that function
return the data object you want saved and recalled. 

Then add `@pipe.AddFunction` above your function to add it to the Pipeline.

Whenever you want to execute your function, just call it and then call `pipe.run()`

```
filterLargeTask(path_to_dataset, filter_parameters) # <-- does not actually run the code, just sets it up to run later

pipe.run() # <-- actually executes the code
```

Presumably we want to do something with this data. In order to recall the data from disk, 
we assign the result of `filterLargeTask` to a variable and pass it along to the next stage 
of the pipeline. 

```
@pipe.AddFunction()
def analyzeTheData(the_data)

    return Analyze(the_data)

my_data = filterLargeTask(path_to_dataset, filter_parameters) # <-- does not actually run the code, just sets it up to run later
analyzeTheData(my_data) # <-- still haven't run anything yet, just getting set up

pipe.run() # <-- skips filterLargeTask, loads the last result it produced from the file system, executes analyzeTheData with the recalled data
``` -->





