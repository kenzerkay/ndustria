import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from ndustria import AddTask, AddView

"""
Create Your Own N-body Simulation (With Python)
Philip Mocz (2020) Princeton Univeristy, @PMocz
Simulate orbits of stars interacting due to gravity
Code calculates pairwise forces according to Newton's Law of Gravity

Modified to allow ndustria to handle simulation and analysis for 
the purposes of demonstrating and testing ndustria's capabilities
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
		self.t = np.zeros(self.Nt) # simulation time at index
		self.dt = dt
		self.softening = softening
		self.G = G
		self.random_seed = random_seed

		self.mass = np.zeros(N)
		self.pos = np.zeros((self.Nt, N, 3))
		self.vel = np.zeros((self.Nt, N, 3))

		self.KE = None
		self.PE = None

	def __str__(self):
		return f"Simulation(N={self.N},tEnd={self.tEnd},dt={self.dt},softening={self.softening},G={self.G},random_seed={self.random_seed})"

	def __repr__(self):
		return str(self)

	def getPosX(self, time_index):
		return self.pos[time_index, :, 0]

	def getPosY(self, time_index):
		return self.pos[time_index, :, 1]

	def getPosZ(self, time_index):
		return self.pos[time_index, :, 2]

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



@AddTask()
def create_initial_conditions(sim):
    # Generate Initial Conditions
	np.random.seed(sim.random_seed)            # set the random number generator seed
	
	sim.mass = 20*np.ones((sim.N,1))/sim.N  # total mass of particles is 20
	pos  = np.random.randn(sim.N, 3)   # randomly selected positions and velocities
	vel  = np.random.randn(sim.N, 3)
	
	# Convert to Center-of-Mass frame
	vel -= np.mean(sim.mass * vel,0) / np.mean(sim.mass)

	result = {
		"pos" : pos,
		"vel" : vel
	}

	return result
	

@AddTask(depends_on=create_initial_conditions)
def run_simulation(initial_conditions, sim ):
	""" N-body simulation """

	print(f"Running simulation with {sim.N} particles...")

	np.copyto(sim.pos[0], initial_conditions["pos"])
	np.copyto(sim.vel[0], initial_conditions["vel"])
	
	# calculate initial gravitational accelerations
	acc = calculate_acceleration( sim , 0)

	# Calculated using Verlet method
	# calculate the first half step velocity outside the loop
	half_step_vel = sim.vel[0] + 0.5 * acc * sim.dt
	
	# Simulation Main Loop
	for i in range(0, sim.Nt-1):

		# calculate position next full step
		sim.pos[i+1] = sim.pos[i] + half_step_vel * sim.dt
		
		# update accelerations
		acc = calculate_acceleration( sim, i+1 )
		
		# calculate velocity next full step
		sim.vel[i+1] = half_step_vel + 0.5 * acc * sim.dt

		# update half step velocity
		half_step_vel += acc * sim.dt
		
		# update time
		sim.t[i+1] = sim.t[i] + sim.dt

		print(f"Simulation time: {sim.t[i+1]:.2f}/{sim.tEnd}", end="\r")

	print("Simulation done.")
	return sim
		
@AddTask(depends_on=run_simulation)
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


	print("Running analysis...")

	# Kinetic Energy:
	sim.KE = np.array(
		[0.5 * np.sum(np.sum( sim.mass * vel**2 )) for vel in sim.vel ] 
	)


	# Potential Energy:
	sim.PE = np.zeros_like(sim.KE)

	for i in range(sim.Nt):
		# positions r = [x,y,z] for all particles
		x = sim.pos[i, :, 0:1]
		y = sim.pos[i, :, 1:2]
		z = sim.pos[i, :, 2:3]	

		# matrix that stores all pairwise particle separations: r_j - r_i
		dx = x.T - x
		dy = y.T - y
		dz = z.T - z

		# matrix that stores 1/r for all particle pairwise particle separations 
		inv_r = np.sqrt(dx**2 + dy**2 + dz**2)
		inv_r[inv_r>0] = 1.0/inv_r[inv_r>0]

		# sum over upper triangle, to count each interaction only once
		sim.PE[i] = sim.G * np.sum(np.sum(np.triu(-(sim.mass * sim.mass.T)*inv_r,1)))

		print(f"Analysis progress: {100*i/sim.Nt:.2f}%", end="\r")
	
	print("Analysis Done                                    ")
	return sim		

@AddView(views=do_analysis)
def view_simulation(sim):

	# prep figure
	plt.ion()

	fig = plt.figure(figsize=(8,10))
	grid = plt.GridSpec(3, 1, wspace=0.0, hspace=0.3)
	ax1 = fig.add_subplot(grid[0:2, 0], projection='3d')
	ax2 = fig.add_subplot(grid[2,0])

	def update(frame):

		plt.sca(ax1)
		plt.cla()
		particles = ax1.scatter(sim.getPosX(frame), sim.getPosY(frame), sim.getPosZ(frame))

		ax1.set(xlim=(-2, 2), ylim=(-2, 2), zlim=(-2, 2))
		ax1.set_aspect('auto', 'box')
		ax1.set_xticks([-2,-1,0,1,2])
		ax1.set_yticks([-2,-1,0,1,2])
		ax1.set_zticks([-2,-1,0,1,2])

		plt.sca(ax2)
		plt.cla()

		KE = ax2.plot(sim.t[:frame], sim.KE[:frame], c='b', label="KE")
		PE = ax2.plot(sim.t[:frame], sim.PE[:frame], c='g', label="PE")
		totalE = ax2.plot(sim.t[:frame], (sim.KE+sim.PE)[:frame], c='k', label="Total")
		ax2.set_xlim(sim.t[0], sim.t[-1])
		ax2.set_ylim(-300, 300)
		plt.legend()

		ax2.set_xlabel("Time")
		ax2.set_ylabel("Energy")	


		return particles, KE, PE, totalE,

	anim = animation.FuncAnimation(fig, update, interval=3, frames=sim.Nt)
	plt.show(block=True)
	
	
# This is mostly for demos but can also be used for testing
@AddView(views=do_analysis)
def virialization(sim):

	fig = plt.figure(figsize=(8,10))
	grid = plt.GridSpec(4, 1, wspace=0.0, hspace=0.5)
	ax1 = fig.add_subplot(grid[0:2, 0], projection='3d')
	ax2 = fig.add_subplot(grid[2,0])
	ax3 = fig.add_subplot(grid[3,0])


	P_over_K = np.abs(sim.PE/sim.KE)

	def update(frame):

		plt.sca(ax1)
		plt.cla()

		particles = ax1.scatter(sim.getPosX(frame), sim.getPosY(frame), sim.getPosZ(frame))

		ax1.set(xlim=(-2, 2), ylim=(-2, 2), zlim=(-2, 2))
		ax1.set_aspect('auto', 'box')
		ax1.set_xticks([-2,-1,0,1,2])
		ax1.set_yticks([-2,-1,0,1,2])
		ax1.set_zticks([-2,-1,0,1,2])

		plt.sca(ax2)
		plt.cla()

		KE = ax2.plot(sim.t[:frame], sim.KE[:frame], c='b', label="KE")
		PE = ax2.plot(sim.t[:frame], sim.PE[:frame], c='g', label="PE")
		totalE = ax2.plot(sim.t[:frame], (sim.KE+sim.PE)[:frame], c='k', label="Total")
		ax2.set_xlim(sim.t[0], sim.t[-1])
		ax2.set_ylim(-300, 300)
		plt.legend()

		ax2.set_xlabel("Time")
		ax2.set_ylabel("Energy")

		plt.sca(ax3)
		plt.cla()

		ratio = ax3.plot(sim.t[:frame], P_over_K[:frame], c='r')
		ax3.set_xlim(sim.t[0], sim.t[-1])
		ax3.set_ylim(0, 5)

		ax3.set_xlabel("Time")
		ax3.set_ylabel("Potential/Kinetic Energy")	
		ax3.hlines(2, 0, sim.tEnd, colors=['k'])

		
		plt.title("Virialization condition is PE/KE = 2")

		return particles, KE, ratio,

	anim = animation.FuncAnimation(fig, update, interval=3, frames=sim.Nt)
	plt.show(block=True)
	


  