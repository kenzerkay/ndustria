from ndustria import Pipeline
from nbody import (
    Simulation,
    create_initial_conditions,
    run_simulation,
    do_analysis,
    view_simulation
)

RERUN = False

if RERUN:
    Pipeline.clearCache()

# Simulation parameters

# For this test, we will vary the number of particles in each
# run of the code, the other parameters will be static
#N         = 100    # Number of particles

t         = 0      # current time of the simulation
tEnd      = 10.0   # time at which simulation ends
dt        = 0.01   # timestep
softening = 0.1    # softening length
G         = 1.0    # Newton's Gravitational Constant
random_seed = 91415 # date of first gravitational wave detection

# Simulate with 100 particles
for i in range(2,3):

    sim = Simulation(
        N=10**i,
        tEnd=tEnd,
        dt=dt,
        softening=softening,
        G=1.0,
        random_seed=random_seed
    )

    ics = create_initial_conditions(sim)
    run_simulation(ics, sim)
    do_analysis()
    view_simulation()


Pipeline.run()