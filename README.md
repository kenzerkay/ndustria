# Industrialize the process of data mining with ndustria! 

# Main features (WIP)

* Simple multiprocess parallelization of arbitrary python code
* Dynamic load balancing
* Caching and reuse of intermediate data products
* Association of code and parameters with data, which updates whenever code updates
* Ease of use and understanding. Designed to work with minimal modification to existing code. 

ndustria is being built as part of a PhD dissertation and is in active development.

# Installation

WIP

# Tutorial

Let's say you have a section of code that runs for a long time located at the top
of your analysis pipeline such that everything downstream of that code has to wait
for it. 

A common example would be filtering a large dataset for a small subset of data. 


```
# this takes a long time to run, 
small_subset = filterLargeDataset(path_to_dataset, filter_parameters)

```

Ndustria implements a wrapper for long-running code that saves the result of that 
work and reuses and recycles it wherever it's needed. 


Here's what wrapping your code looks like
```
@AddTask() # <-- 1. function decorator
def FilterLargeTask(): # <-- 2. wrapper function 

    # this takes a long time to run, 
    small_subset = filterLargeDataset(path_to_dataset, filter_parameters)

    return small_subset #<-- 3. return statement with data object we want saved
```

First, wrap your long-running code in a python function and have that function
return the data object you want saved and recalled. 

Then add the AddTask function decorator to register your function as an 
Ndustria Task. 





