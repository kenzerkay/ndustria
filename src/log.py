
DEBUG = True

# Convenience debugger function
# prints stuff out to log and console with [Debug] in front of it 
# when DEBUG is set to True
# Should be used when specifically trying to 
# figure out what the fuck is going wrong
def debug(args):

    if DEBUG:
        log("[Debug] " + str(args))

# Convenience debugger function
# prints stuff out to log and console with [Warning] in front of it 
# when DEBUG is set to True
def warn(args):
    msg = "[Warning] " + str(args)
    log(msg)
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

    pass