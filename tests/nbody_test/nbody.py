import numpy as np
import matplotlib.pyplot as plt

"""
Create Your Own N-body Simulation (With Python)
Philip Mocz (2020) Princeton Univeristy, @PMocz
Simulate orbits of stars interacting due to gravity
Code calculates pairwise forces according to Newton's Law of Gravity

Modified to allow ndustria to handle simulation and analysis for 
the purposes of demonstrating ndustria's capabilities
"""

class Simulation:
	"""Class that stores simulation parameters and data"""
	def __init__(self, 
		N         = 100,     # number of particles 
		tEnd      = 10.0,    # time at which simulation ends
		dt        = 0.01,    # timestep
		softening = 0.1,     # softening length
		G         = 1.0,     # Newton's Gravitational Constant
		random_seed = 91415 # random seed for initial conditions
	):

		self.N = N
		self.tEnd = tEnd		
		self.Nt = int(np.ceil(tEnd/dt)) # number of timesteps
		self.dt = dt
		self.softening = softening
		self.G = G
		self.random_seed = random_seed

		self.mass = np.zeros(N)
		self.pos = np.zeros(self.Nt, N, 3)
		self.vel = np.zeros(self.Nt, N, 3)

	def __str__(self):
		return f"Simulation(N={self.N},tEnd={self.tEnd},dt={self.dt},softening={self.softening},G={self.G},random_seed={self.random_seed})"

	def __repr__(self):
		return str(self)

def calculate_acceleration( sim, iteration ):
	"""
    Calculate the acceleration on each particle due to Newton's Law 
	pos  is an N x 3 matrix of positions
	mass is an N x 1 vector of masses
	G is Newton's Gravitational constant
	softening is the softening length
	a is N x 3 matrix of accelerations
	"""
	# positions r = [x,y,z] for all particles
	x = sim.pos[iteration, :, 0:1]
	y = sim.pos[iteration, :, 1:2]
	z = sim.pos[iteration, :, 2:3]

	# matrix that stores all pairwise particle separations: r_j - r_i
	dx = x.T - x
	dy = y.T - y
	dz = z.T - z

	# matrix that stores 1/r^3 for all particle pairwise particle separations 
	inv_r3 = (dx**2 + dy**2 + dz**2 + sim.softening**2)
	inv_r3[inv_r3>0] = inv_r3[inv_r3>0]**(-1.5)

	ax = sim.G * (dx * inv_r3) @ sim.mass
	ay = sim.G * (dy * inv_r3) @ sim.mass
	az = sim.G * (dz * inv_r3) @ sim.mass
	
	# pack together the acceleration components
	a = np.hstack((ax,ay,az))

	return a

#@AddTask()
def do_analysis( sim ):
	"""
	Get kinetic energy (KE) and potential energy (PE) of simulation
	pos is N x 3 matrix of positions
	vel is N x 3 matrix of velocities
	mass is an N x 1 vector of masses
	G is Newton's Gravitational constant
	KE is the kinetic energy of the system
	PE is the potential energy of the system
	"""
	# Kinetic Energy:
	KE = 0.5 * np.sum(np.sum( sim.mass * sim.vel**2 ))


	# Potential Energy:

	# positions r = [x,y,z] for all particles
	x = sim.pos[:,0:1]
	y = sim.pos[:,1:2]
	z = sim.pos[:,2:3]

	# matrix that stores all pairwise particle separations: r_j - r_i
	dx = x.T - x
	dy = y.T - y
	dz = z.T - z

	# matrix that stores 1/r for all particle pairwise particle separations 
	inv_r = np.sqrt(dx**2 + dy**2 + dz**2)
	inv_r[inv_r>0] = 1.0/inv_r[inv_r>0]

	# sum over upper triangle, to count each interaction only once
	PE = sim.G * np.sum(np.sum(np.triu(-(sim.mass * sim.mass.T)*inv_r,1)))
	
	return KE, PE

#@AddTask()
def create_initial_conditions(sim):
    # Generate Initial Conditions
	np.random.seed(sim.random_seed)            # set the random number generator seed
	
	sim.mass = 20.0*np.ones((sim.N,1))/sim.N  # total mass of particles is 20
	pos  = np.random.randn(sim.N, 3)   # randomly selected positions and velocities
	vel  = np.random.randn(sim.N, 3)
	
	# Convert to Center-of-Mass frame
	vel -= np.mean(sim.mass * vel,0) / np.mean(sim.mass)

	result = {
		"pos" : pos,
		"vel" : vel
	}

	return result
	

#@AddTask()
def run_simulation(initial_conditions, sim ):
	""" N-body simulation """

	np.copyto(sim.pos[0], initial_conditions["pos"])
	np.copyto(sim.vel[0], initial_conditions["vel"])
	
	# calculate initial gravitational accelerations
	acc = calculate_acceleration( sim )
	
	# Simulation Main Loop
	for i in range(sim.Nt):

		# Calculated using leap frog method
		
		# (1/2) kick
		sim.vel[i+1] = sim.vel[i] + acc * sim.dt/2.0
		
		# drift
		pos += vel * sim.dt
		
		# update accelerations
		acc = calculate_acceleration( sim, i )
		
		# (1/2) kick
		vel += acc * sim.dt/2.0
		
		# update time
		t += sim.dt
		
		
#@AddView()
def view_simulation():

	# prep figure
	fig = plt.figure(figsize=(4,5), dpi=80)
	grid = plt.GridSpec(3, 1, wspace=0.0, hspace=0.3)
	ax1 = plt.subplot(grid[0:2,0])
	ax2 = plt.subplot(grid[2,0])
		
	# plot in real time
	if plotRealTime or (i == Nt-1):
		plt.sca(ax1)
		plt.cla()
		xx = pos_save[:,0,max(i-50,0):i+1]
		yy = pos_save[:,1,max(i-50,0):i+1]
		plt.scatter(xx,yy,s=1,color=[.7,.7,1])
		plt.scatter(pos[:,0],pos[:,1],s=10,color='blue')
		ax1.set(xlim=(-2, 2), ylim=(-2, 2))
		ax1.set_aspect('equal', 'box')
		ax1.set_xticks([-2,-1,0,1,2])
		ax1.set_yticks([-2,-1,0,1,2])
		
		plt.sca(ax2)
		plt.cla()
		plt.scatter(t_all,KE_save,color='red',s=1,label='KE' if i == Nt-1 else "")
		plt.scatter(t_all,PE_save,color='blue',s=1,label='PE' if i == Nt-1 else "")
		plt.scatter(t_all,KE_save+PE_save,color='black',s=1,label='Etot' if i == Nt-1 else "")
		ax2.set(xlim=(0, tEnd), ylim=(-300, 300))
		ax2.set_aspect(0.007)
		
		plt.pause(0.001)
	    
	
	
	# add labels/legend
	plt.sca(ax2)
	plt.xlabel('time')
	plt.ylabel('energy')
	ax2.legend(loc='upper right')
	
	# Save figure
	plt.savefig('nbody.png',dpi=240)
	plt.show()
	    
	return 0
	


  
if __name__== "__main__":
  main()