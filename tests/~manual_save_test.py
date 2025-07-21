from ndustria import Pipeline
import numpy as np

pipe = Pipeline()

@pipe.AddFunction()
def create_a_file_with_word(word, index):

    filename = f"test_file_{index}.txt"
    with open(filename, "w") as f:
        f.write(word)
    return filename

@pipe.AddFunction()
def test_file_was_created(filename, original_word):

    with open(filename, "r") as f:
        word = f.read()
    
    print(f"Reading file {filename}")
    print(f"Found the word '{word}'")

    if word == original_word:
        print(f"This matches the original word, '{original_word}'")
    else: 
        print(f"This does not match the original word, '{original_word}'")

    return "no_result"


words = [
    "apple",
    "banana",
    "orange",
    "pear",
    "grapes"
]

for i,w in enumerate(words):
    file = create_a_file_with_word(w,i)
    test_file_was_created(file, w)


pipe.run(run_all=True)
