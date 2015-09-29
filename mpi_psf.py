#PSF only
import py3shape.i3meds
import py3shape.options
import py3shape.image
import os
import numpy as np
import desnerscdb



class PSFProcess(MPIProcess):
    #Functions you must overwrite
    def make_tasks(self):
        results = []

        meds_files = [line.strip() for line in open(self.meds_list) if line.strip() and not line.strip().startswith('#')]

        for meds_file in meds_files:
            m = py3shape.i3meds.I3MEDS(meds_file, blacklist=self.options.blacklist)
            iobjs = np.arange(m.size)
            chunks = np.array_split(iobjs, size)
            for chunk in chunks:
                results.append((meds_file, chunk))

        return results

    def run_tasks(self, tasks):
        #this is a chunk of tasks
        filename, iobjs = tasks
        if filename!=self.filename:
            self.meds = py3shape.i3meds.I3MEDS(filename, blacklist=self.options.blacklist)

        cat = self.meds.get_cat()
        results = [filename]

        for iobj in iobjs:
            n_image = cat['ncutout'][iobj]
            iexps = self.meds.select_exposures(iobj, self.options, 1, n_image)
            for iexp in iexps:
                img = m.get_bundled_psfex_psf(iobj, iexp, self.options)
                psf = py3shape.image.Image(img)
                mom = psf.weighted_moments(weight_radius=10.)
                e1 = mom.e1
                e2 = mom.e2
                e1_sky, e2_sky = m.convert_g_image2sky(iobj, iexp, options.stamp_size, e1, e2)
                ID = cat['id'][iobj]
                results.append((ID, e1, e2, e1_sky, e2_sky))
        return results

    def write_output(self, results):
        filename = results[0]
        results = results[1:]
        cursor = self.connection.cursor()
        for result in results:
            cursor.execute("insert into spteg_psf values (%s,%s,%s,%s,%s)", result)
        self.connection.commit()
        cursor.close()

    def master_setup(self):
        #connect to database
        self.connection = desnerscdb.connect()
        self.meds_list = args['list']

    def slave_setup(self):
        self.options = py3shape.options.Options()
        self.options.read()
        self.options.read(args["ini"])
        self.meds = None
        self.filename = ""


if __name__ == '__main__':
    args = {'ini':'psf.ini', 'list':'meds.txt'}
    debug_mpi = True
    PSFProcess.main_loop(debug_mpi, args)
