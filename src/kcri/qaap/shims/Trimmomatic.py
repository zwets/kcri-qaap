#!/usr/bin/env python3
#
# kcri.qaap.shims.Trimmomatic - service shim to the Trimmomatic backend
#

import os, logging
from pico.workflow.executor import Execution
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "Trimmomatic", DEPS_VERSIONS['trimmomatic']

# Resource parameters: cpu, memory, disk, run time reqs
MAX_CPU = 1
MAX_MEM = 1
MAX_SPC = 1
MAX_TIM = 10 * 60

# The Service class
class TrimmomaticShim:
    '''Service shim that executes the backend.'''

    def execute(self, ident, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Execution.'''

        execution = TrimmomaticExecution(SERVICE, VERSION, ident, blackboard, scheduler)

        # Get the execution parameters from the blackboard
        try:
            if len(execution.get_fastq_paths()) != 2:
                raise UserException("Trimmomatic backend only handles paired-end reads")

            params = [
                '--cores', MAX_CPU,
                '--memory', MAX_MEM,
                '--reads', ','.join(execution.get_fastq_paths()),
                '--contigs_out', CONTIGS_OUT
            ]

            job_spec = JobSpec('trimmomatic', params, MAX_CPU, MAX_MEM, MAX_SPC, MAX_TIM)
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
class TrimmomaticExecution(ServiceExecution):
    '''A single execution of the service, returned by execute().'''

    _job = None

    def start(self, job_spec):
        if self.state == Execution.State.STARTED:
            self._job = self._scheduler.schedule_job('trimmomatic', job_spec, 'Trimmomatic')

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        contigs_file = job.file_path(CONTIGS_OUT)

        if os.path.isfile(contigs_file):
            self.store_results({ 'contigs_file': contigs_file })
            self._blackboard.put_assembled_contigs_path(contigs_file)
        else:
            self.fail("backend job produced no output, check: %s", job.file_path(""))

