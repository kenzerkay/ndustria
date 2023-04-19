import os
DEBUG = True
VERBOSE = True
LOG_FILE = ""
# Convenience debugger function
# prints stuff out to log and console with [Debug] in front of it 
# when DEBUG is set to True
# Should be used when specifically trying to 
# figure out what the fuck is going wrong
def debug(args):

    if not DEBUG: return

    msg = "[Debug] " + str(args)
    log(msg)

    if VERBOSE == True:
        print(msg)
    

# Convenience debugger function
# prints stuff out to log with [Warning] in front of it 
# when DEBUG is set to True
def warn(args):
    msg = "[Warning] " + str(args)
    log(msg)

    if VERBOSE == True:
        print(msg)


# Convenience error function
# for when you need to die but also try to explain to the user 
# why you died
# outputs to both log and console
def error(args):

    msg = "[Error] " + str(args)
    log(msg)
    print(msg)
    exit()


# Convenience error function
# for reporting on the normal functioning of the code
def log(msg):
    global LOG_FILE 
    if LOG_FILE == "":
        print(msg)
        return

    with open(LOG_FILE, "a+") as log:
        print(msg, file=log)

def setLogFile(filepath):
    global LOG_FILE 
    LOG_FILE = os.path.abspath(filepath)
    with open(filepath, "w") as log:
        pass