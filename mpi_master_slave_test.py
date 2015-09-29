from  mpi_master_slave import MPIProcess

class TestProcess(MPIProcess):
	#Functions you must overwrite
	def make_tasks(self):
		return [range(10*i,10*i+10) for i in xrange(self.njob)]

	def run_tasks(self, tasks):
		for task in tasks:
			yield self.coeff*task

	def write_output(self, results):
		print results

	def master_setup(self):
		self.njob = self.args['njob']

	def slave_setup(self):
		self.coeff = self.args['coeff']



if __name__ == '__main__':
	import mpi4py.MPI
	world = mpi4py.MPI.COMM_WORLD
	debug=True
	args = {"njob":5, "coeff":3}
	TestProcess.main_loop(world, debug, args)