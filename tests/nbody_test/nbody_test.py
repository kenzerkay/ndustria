from ndustria import Pipeline
from nbody import (
    Simulation,
    create_initial_conditions,
    run_simulation,
    do_analysis,
    view_simulation,
    virialization
)


# Simulation parameters
N         = 100    # Number of particles
t         = 0      # current time of the simulation
tEnd      = 10.0   # time at which simulation ends
dt        = 0.01   # timestep
softening = 0.1    # softening length
G         = 1.0    # Newton's Gravitational Constant
random_seed = 91415 # date of first gravitational wave detection


sim = Simulation(
    N=N,
    tEnd=tEnd,
    dt=dt,
    softening=softening,
    G=G,
    random_seed=random_seed
)

ics = create_initial_conditions(sim)
sim_data = run_simulation(ics, sim)
analysis = do_analysis(sim_data)
#view_simulation()
virialization(analysis)


Pipeline.run(timeit=True, memcheck=True)