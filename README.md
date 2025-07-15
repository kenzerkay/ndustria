# Industrialize the process of data mining with ndustria! 

# Main features (WIP)

* Simple multiprocess parallelization of arbitrary python code
* Dynamic load balancing
* Caching and reuse of intermediate data products
* Association of code and parameters with data, which updates whenever code updates
* Ease of use and understanding. Designed to work with minimal modification to existing code. 

ndustria is being built as part of a PhD dissertation and is in active development.

# Installation

* Fork this repository on Github
* Download your version with 

`git clone <YOUR_VERSION_URL>`

* Add `PYTHONPATH` and `PATH` to your `.bashrc` file 

```
export PYTHONPATH="$PYTHONPATH:$HOME"
export PATH="$PATH:$HOME/ndustria/bin"
```

**MAKE SURE: `PATH` points to ndustria's bin and `PYTHONPATH` points to the installation of ndustria**

* Just outside of the ndustria directory run  

```
python -m ndustria.__init__
```
* Don't forget to run `  ndustria --setup` to save your cache directory 


# Tutorial

Let's say you have a section of code that runs for a long time located at the top
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
```





