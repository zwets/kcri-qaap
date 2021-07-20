#!/usr/bin/env python3
#
# kcri.qaap.shims.FastQScreen - implements the FastQScreenShim
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS
from ..workflow import Services

# Our service name and current backend version
SERVICE, VERSION = "FastQScreen", DEPS_VERSIONS['fastq-screen']


class FastQScreenShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        execution = FastQScreenExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

         # Get the execution parameters from the blackboard
        try:
            fastqs = execution.get_input_fastqs().values() if Services(sid) == Services.FASTQSCREEN else \
                     execution.get_output_fastqs().values() if Services(sid) == Services.POST_FASTQSCREEN else \
                     None

            if fastqs is None: raise Exception('unknown ident in FastQScreenShim: %s' % sid.value)
            if not fastqs: raise UserException('no fastq files to process')

            # Compute resources
            n_fq = len(fastqs)
            max_par = int(execution._scheduler.max_mem * 4)    # each thread needs 250MB
            cpu = min(execution._scheduler.max_cpu, len(fastqs), max_par)
            mem = cpu / 4               # each job 250M
            spc = n_fq / 10             # each job at most 100M
            tim = n_fq / cpu * 5 * 60   # each job at most 5 min

            # Set up parameters
            params = [
                '--outdir', '.',
                '--force',
                '--quiet',
                '--threads', cpu
            ]

            params.extend(fastqs)

            job_spec = JobSpec('fastq_screen', params, cpu, mem, spc, tim)
            execution.store_job_spec(job_spec.as_dict())
            execution.start(job_spec)

        # Failing inputs will throw UserException
        except UserException as e:
            execution.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            execution.fail(str(e))

        return execution


# Single execution of the service
class FastQScreenExecution(ServiceExecution):
    '''A single execution of the Quast service'''

    _job = None

    def start(self, job_spec):
        if self.state == Task.State.STARTED:
            self._job = self._scheduler.schedule_job('fastq-screen', job_spec, self.sid)

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        # In all cases, store FastQScreen output path
        self.store_results(dict(output_path = job.file_path("")))

        # FastQScreen doesn't report errors using its exit code (sigh), so read its stderr
        try:
            fail = False
            with open(job.stderr, 'r') as f:
                for l in f:
                    fail = True
                    self.add_error('fastq-screen: %s' % l.strip())
            if fail:
                self.fail('FastQScreen reported errors')

        except Exception as e:
            self.fail("failed to parse error output (%s): %s" % (job.stderr, str(e)))

