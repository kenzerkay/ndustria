from ndustria import Pipeline
from nbody import (
    Simulation,
    create_initial_conditions,
    run_simulation,
    do_analysis,
    softening_length_test
)


# Simulation parameters
N         = 250    # Number of particles
t         = 0      # current time of the simulation
tEnd      = 10.0   # time at which simulation ends
dt        = 0.01   # timestep
softening = 0.1    # softening length
G         = 1.0    # Newton's Gravitational Constant
random_seed = 91415 # date of first gravitational wave detection

analysis_results = []
for soft in [0.5, 0.1, 0.001]:

    sim = Simulation(
        N=N,
        tEnd=tEnd,
        dt=dt,
        softening=soft,
        G=G,
        random_seed=random_seed
    )

    ics = create_initial_conditions(sim)
    sim_data = run_simulation(ics, sim)
    analysis_results.append(do_analysis(sim_data)) 
# end main loop

softening_length_test(analysis_results)


Pipeline.run(parallel=True, rerun=True)