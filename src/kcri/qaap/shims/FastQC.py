#!/usr/bin/env python3
#
# kcri.qaap.shims.FastQC - service shim to the FastQC backend
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS
from ..workflow import Services

# Our service name and current backend version
SERVICE, VERSION = "FastQC", DEPS_VERSIONS['fastqc']


class FastQCShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = FastQCExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

         # Get the task parameters from the blackboard
        try:
            fastqs = task.get_input_fastqs().values() if Services(sid) == Services.FASTQC else \
                     task.get_output_fastqs().values() if Services(sid) == Services.POST_FASTQC else \
                     None

            if fastqs is None: raise Exception('unknown service in FastQCShim: %s' % sid)
            if not fastqs: raise UserException('no fastq files to process')

            # Compute resources
            n_fq = len(fastqs)
            max_par = int(task._scheduler.max_mem * 4)    # each thread needs 250MB
            cpu = min(task._scheduler.max_cpu, len(fastqs), max_par)
            mem = cpu / 4               # each job 250M
            spc = n_fq / 10             # each job at most 100M
            tim = n_fq / cpu * 5 * 60   # each job at most 5 min

            # Set up parameters
            params = [
                '--outdir', '.',
                '--noextract',
                '--quiet',
                '--threads', cpu
            ]

            params.extend(fastqs)

            job_spec = JobSpec('fastqc', params, cpu, mem, spc, tim)
            task.store_job_spec(job_spec.as_dict())
            task.start(job_spec)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task


# Single execution of the service
class FastQCExecution(ServiceExecution):
    '''A single execution of the Quast service'''

    _job = None

    def start(self, job_spec):
        if self.state == Task.State.STARTED:
            self._job = self._scheduler.schedule_job('fastqc-%s' % self.sid, job_spec, self.sid)

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        # In all cases, store FastQC output path
        self.store_results(dict(output_path = job.file_path("")))

        # FastQC doesn't report errors using its exit code (sigh), so read its stderr
        try:
            fail = False
            with open(job.stderr, 'r') as f:
                for l in f:
                    fail = True
                    self.add_error('fastqc: %s' % l.strip())
            if fail:
                self.fail('FastQC reported errors')

        except Exception as e:
            self.fail("failed to parse error output (%s): %s" % (job.stderr, str(e)))

