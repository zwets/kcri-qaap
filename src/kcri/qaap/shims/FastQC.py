#!/usr/bin/env python3
#
# kcri.qaap.shims.FastQC - service shim to the FastQC backend
#

import os, csv, logging
from pico.workflow.executor import Execution
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "FastQC", '0.11.9' #DEPS_VERSIONS['0.11.9']

# Backend resource parameters: cpu, memory, disk, run time reqs
MAX_CPU = 2
MAX_MEM = 1
MAX_SPC = 1
MAX_TIM = 5 * 60


class FastQCShim:
    '''Service shim that executes the backend.'''

    def execute(self, ident, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Execution.'''

        # Check for fastqs else throw to SKIP execution
        fastqs = blackboard.get_fastq_paths()
        if not fastqs: raise UserException("FastQC requires FASTQ or BAM/SAM files")

        execution = FastQCExecution(SERVICE, VERSION, ident, blackboard, scheduler)

         # Get the execution parameters from the blackboard
        try:
            # Set up parameters
            params = [
                '--outdir', '.',
                '--extract',
                '--quiet'
            ]

            params.extend(fastqs)

            job_spec = JobSpec('fastqc', params, MAX_CPU, MAX_MEM, MAX_SPC, MAX_TIM)
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
class FastQCExecution(ServiceExecution):
    '''A single execution of the Quast service'''

    _job = None

    def start(self, job_spec):
        if self.state == Execution.State.STARTED:
            self._job = self._scheduler.schedule_job('fastqc', job_spec, 'FastQC')

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

#        tsv = job.file_path('report.tsv')
        try:
#            metrics = dict()
#
#            with open(tsv, newline='') as f:
#                reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
#                for row in reader:
#                    metrics[translate.get(row[0], row[0])] = row[1]
#
            self.store_results({
                'see_here': job.file_path("")
                })
            
        except Exception as e:
            #self.fail("failed to parse output file %s: %s" % (tsv, str(e)))
            self.fail("FastQC output processing TBD: %s" % str(e))

