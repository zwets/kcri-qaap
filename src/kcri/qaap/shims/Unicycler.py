#!/usr/bin/env python3
#
# kcri.qaap.shims.Unicycler - service shim to the Unicycler backend
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS


# Our service name and current backend version
SERVICE, VERSION = "Unicycler", DEPS_VERSIONS['unicycler']

# Resource parameters per job, see below
MAX_SPC = 1
MAX_TIM = 60*60


# The Service class
class UnicyclerShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = UnicyclerExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)


        # Get the task parameters from the blackboard
        try:
            il_fqs = blackboard.get_input_il_fqs()
            if not il_fqs: raise UserException("no Illumina reads to process, skipping Unicycler")

            # Assume single job requires 15G memory
            job_mem = 15
            # Max number of parallel jobs that would fit memory, assuming job_mem per job
            mem_cap = int(max(scheduler.max_mem / job_mem, 1))
            # Max out the CPU per job but give each at least 8 (or the max if less)
            job_cpu = int(max(scheduler.max_cpu / min(mem_cap, len(il_fqs)), min(scheduler.max_cpu, 8)))

            # TODO: 
            # - assemble single fastq (trivial to add), and/or
            # - add unpaired or long reads (so how to recognise, and consider TryCycler instead)

            task.start(il_fqs, job_cpu, job_mem)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task

# Single execution of the service
class UnicyclerExecution(MultiJobExecution):
    '''The single execution of service sid in workflow execution _xid (None).
       Schedules a job for every illumina paired end read in the xid.'''

    def start(self, pairs, req_cpu, req_mem):

        mode = self._blackboard.get_user_input('un_m')
        polish = self._blackboard.get_user_input('un_p')

        if self.state == Task.State.STARTED:
            for fid,(r1,r2) in pairs.items():

                params = [
                    # Need to override SPAdes path, Unicycler requires old version
                    '--spades_path', '/usr/src/ext/spades-uni/bin/spades.py',
                    #'--spades_tmp_dir', '/tmp', # default is tmp dir in out dir
                    '--threads', req_cpu,
                    '--min_polish_size', polish,
                    '--mode', mode,
                    '-1', r1,
                    '-2', r2,
                    #'-s', unpaired
                    '-o', '.'
                ]

                job_spec = JobSpec('unicycler', params, req_cpu, req_mem, MAX_SPC, MAX_TIM)
                self.store_job_spec(job_spec.as_dict())
                self.add_job('unicycler-%s' % fid, job_spec, '%s/%s' % (self.sid, fid), fid)

    def collect_job(self, results, job, fid):
        '''Collect the output for this job.'''

        res = dict()
        results[fid] = res

        gfa = job.file_path('assembly.gfa')
        fna = job.file_path('assembly.fasta')
        log = job.file_path('unicycler.log')

        if os.path.isfile(gfa):
            res['graph'] = gfa

        if os.path.isfile(fna):
            res['fasta'] = fna
            self._blackboard.add_assembled_fasta(fid, fna)
        else:
            self.fail("backend job produced no assembly, check: %s", job.file_path(""))

        # Parse assembly information from unicycler's log if found

        if not os.path.isfile(log):
            return
 
        with open(log) as f:

            # Skip to the 'Bridged assembly' section
            l = ''
            while f and not l.startswith('Bridged assembly'): l = next(f)
            for _ in range(0,7): l = next(f) if f else ''

            #Component   Segments   Links   Length   N50     Longest segment   Status
            #    total          8       1   19,701   2,569             6,706
            #        1          1       0    6,706   6,706             6,706   incomplete
            #        2          1       1    3,133   3,133             3,133     complete

            # Parse the "total" line into a dict
            s = l.replace(',','').split()
            if not s: return

            res['stats'] = dict({'total': {
                'length': s[3],
                'segments': s[1],
                'links': s[2],
                'n1': s[5],
                'n50': s[4]}})

            cs = list()
            res['stats']['components'] = cs

            # Parse the components lines into a list of dict
            s = next(f).replace(',','').split()
            while s:
                cs.append(dict({
                    'comp': s[0],
                    'length': s[3],
                    'segs': s[1],
                    'links': s[2],
                    'n1': s[5],
                    'n50': s[4],
                    'complete': s[6] == "complete" if len(s) > 4 else 'unknown'}))
                s = next(f).replace(',','').split() if f else list()

