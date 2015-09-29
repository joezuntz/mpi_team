#mpi_master_slave.py
import sys

END_OF_JOBS = "END_OF_JOBS_SENTINEL"
END_OF_TASK = "ENDTASK"
intern(END_OF_TASK)
intern(END_OF_JOBS)




class MPIProcess(object):
	def __init__(self, comm, debug_mpi, args):
		import mpi4py.MPI
		self.MPI = mpi4py.MPI
		self.comm = comm
		self.args = args
		self.debug_mpi = debug_mpi
		self.setup()



	#Functions you must overwrite
	def make_tasks(self):
		#only called on master
		raise RuntimeError("Must implement make_tasks on master")

	def run_tasks(self, tasks):
		#only called on slave. collection of tasks
		raise RuntimeError("Must implement run_tasks on slave")

	#Functions you probably want to overwrite
	def write_output(self, results):
		#only called on master
		print results

	def master_setup(self):
		#only called on master
		pass

	def slave_setup(self):
		#only called on slave
		pass


	#Main method you call from application:

	@classmethod
	def main_loop(cls, debug_mpi, args, comm=None):
		if comm is None:
			comm = cls.get_world()
		proc = cls(comm, debug_mpi, args)
		proc.run()



	@staticmethod
	def get_world(): #not enough!
		import mpi4py.MPI
		world = mpi4py.MPI.COMM_WORLD
		return world



	#The rest you can probably leave alone ...
	def is_master(self):
		return self.comm.Get_rank()==0


	def setup(self):
		if self.is_master():
			self.master_setup()
		else:
			self.slave_setup()


	def run_master(self):
		args = self.args
		comm = self.comm

		size = comm.Get_size()

		#Make a list of the jobs to do
		tasks = self.make_tasks()


		n_tasks = len(tasks)
		completed_tasks = 0
		launched_tasks = 0
		print "Have %d total tasks for %d procs" % (n_tasks, size-1)

		#Send out first batch of tasks, one per proc
		initial_batch_size = min(n_tasks, size-1)
		for i in xrange(initial_batch_size):
			task = tasks[i]
			comm.send(task, dest=i+1)
			print "Task %d/%d for proc %d: %s" % (i+1, n_tasks, i+1, task)
			launched_tasks+=1

		#In small jobs not every process will get a task at all
		#particularly if we are just finishing up.
		#in that case end the unwanted jobs immediately
		if initial_batch_size<size-1:
			for i in xrange(initial_batch_size, size-1):
				comm.send(END_OF_JOBS, dest=i+1)
				print "Closing task %d straight off (not needed)" % (i+1)

		#Listen for results
		while completed_tasks < n_tasks:
			status = self.MPI.Status()
			results = comm.recv(source=self.MPI.ANY_SOURCE, tag=self.MPI.ANY_TAG, status=status)

			if isinstance(results, str) and results==END_OF_TASK:
				completed_tasks+=1

				proc = status.Get_source()
				print "Finished task from proc %d" % proc

				if launched_tasks<n_tasks:
					#We have more jobs - send the next one out
					task = tasks[launched_tasks]
					print "Task %d/%d for proc %d: %s" % (launched_tasks, n_tasks, proc, task)
					launched_tasks+=1
					comm.send(task, dest=proc)
				else:
					#All done - tell the job to finish
					comm.send(END_OF_JOBS, dest=proc)
					print "Closing proc %d" % proc
			else:
				if self.debug_mpi:
					print "Master received result from proc %d" % status.Get_source()
				self.write_output(results)
		print "Master: all jobs complete"

	def run_slave(self):
		args = self.args
		comm = self.comm
		rank = comm.Get_rank()

		#main program loop
		while True:
			#debug output
			if self.debug_mpi:
				print "Proc %d waiting for job"%rank
			#task to do
			tasks = comm.recv(source=0)

			#check if this is the end of jobs sentinel
			if isinstance(tasks,str) and  tasks==END_OF_JOBS:
				break

			#Send jobs back to master for output
			for result in self.run_tasks(tasks):
				comm.send(result, dest=0)

			#debug output
			if self.debug_mpi:
				print "Proc %d task complete." % rank

			#all done; request new task or shutdown info
			comm.send(END_OF_TASK, dest=0)

	def run(self):
		if self.is_master():
			self.run_master()
		else:
			self.run_slave()








# class MedsMaster(MPIMaster):
# 	def write_output(self, results):
# 		main, epoch=results
# 		self.output.write_row(main, epoch)

# 	def setup(self):
# 		options=py3shape.Options(self.args.ini)
# 		if options.database_output:
# 			output = MPIDatabaseOutput(self.comm, self.args.output, 
# 				hostname=options.database_host, database=options.database_name)
# 		else:
# 			output = MPITextOutput(comm, args.output+"_base.txt", args.output+"_epoch.txt")
# 		self.output = output

# 	def make_tasks(self):
# 		args = self.args
# 		output = self.output
# 		if args.nbc:
# 			return make_nbc_tasks(args)

# 		tasks = []
# 		for line in open(args.meds_list):
# 			#ignore empty or commented lines
# 			line=line.strip()
# 			if (not line) or line.startswith("#"): continue
# 			meds_files=line.split()
# 			meds_file = meds_files[0]
			
# 			if args.cat=="all":
# 				#Just use all the objects
# 				iobjs = np.arange(meds.MEDS(meds_file).size)
# 			else:
# 				#Hijack the connection to the DB to check for objects already in the DB
# 				iobjs = select_objects(meds_file, args.cat, output, args.great ,args.multiband)
# 			print "Found %d objects in %s" % (len(iobjs), meds_file)
# 			if args.limit:
# 				iobjs=iobjs[:args.limit]
# 			nobj = len(iobjs)
# 			if nobj==0: continue
# 			nchunk = nobj//args.chunk
# 			if nchunk==0: nchunk=1
# 			chunks = np.array_split(iobjs, nchunk)
# 			if args.cat=="all" and args.multiband:
# 				raise ValueError("Cannot do multiband without a catalog right now.")
# 			for chunk in chunks:
# 				task = (meds_files, chunk)
# 				tasks.append(task)
# 		return tasks

# class MedsSlave(object):
# 	def setup(self):
# 		comm = self.comm
# 		args = self.args

# 		options=py3shape.Options(args.ini)

# 		if args.nbc:
# 			raise ValueError("NBC meds_mpi is broken")
# 			generator = I3GalsimInput(options.stamp_size, options.nbc_exposures)		
# 			analysis = I3GalsimAnalysis(args)
# 		elif args.multi:
# 			analysis = I3MultiMedsAnalysis(args)
# 		else:
# 			analysis = I3MedsAnalysis(args)

# 		self.analysis = analysis

# 		#slave output just forwards to master
# 		if options.database_output:
# 			output = MPIDatabaseOutput(comm, None, hostname=options.database_host, database=options.database_name)
# 		else:
# 			output = MPITextOutput(comm, args.output+"_base.txt", args.output+"_epoch.txt")

# 		self.output = output

# 	def run_task(self, task):
# 		#otherwise, we have been sent the name of the 
# 		#meds file and a lsit of objects to run un
# 		meds_files, iobjs = task
# 		if self.args.nbc:
# 			self.analysis.main(generator, iobjs, self.output, 1, 0, args.fatal_errors)
# 			self.analysis.main(meds_files[0], iobjs, self.output, 1, 0, args.fatal_errors)
# 		elif self.args.multi:
# 			#iobjs is not really iobjs, it is coadd_objects_id in this case
# 			self.analysis.main(meds_files, iobjs, self.output, 1, 0, args.fatal_errors)
# 		else:
# 			#Might need to look up a precomputed result here.
# 			#Best way: load from FITS file, then fill in options
# 			#object with results and set minimizer_loops=-1
# 			self.analysis.main(meds_files[0], iobjs, self.output, 1, 0, args.fatal_errors)

# 			#should yield results to master here

# 			