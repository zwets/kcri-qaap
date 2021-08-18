#!/usr/bin/env python3
#
# kcri.qaap.shims.ContigsMetrics - service shim to the uf-stats backend
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "ContigsMetrics", DEPS_VERSIONS['unfasta']

# Resource parameters: cpu, memory, disk, run time reqs
MAX_CPU = 2
MAX_MEM = 1
MAX_TIM = 5 * 60


# The Service class
class ContigsMetricsShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = ContigsMetricsExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

        # From here run the execution, and FAIL it on exception
        try:
            fastas = task.get_all_fastas()
            if not fastas: raise UserException('no FASTA files to process')

            task.start(fastas)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task

# Single execution of the service
class ContigsMetricsExecution(MultiJobExecution):
    '''The single execution of service sid in workflow execution _xid (None).
       Schedules a job for every fasta file in the xid.'''

    def start(self, fastas):
        if self.state == Task.State.STARTED:
            for fid, fa in fastas.items():

                # Cater for either gzipped or plain input using shell succinctness
                cmd = "(gzip -dc '%s' 2>/dev/null || cat '%s') | uf | uf-stats -t" % (fa,fa) 
                job_spec = JobSpec('sh', [ '-c', cmd, 'uf-stats' ], MAX_CPU, MAX_MEM, MAX_TIM)

                # We add the fid as userdata, so we can use it in collect_output
                self.store_job_spec(job_spec.as_dict())
                self.add_job('uf-stats-%s' % fid, job_spec, '%s/%s' % (self.sid, fid), fid)

    def collect_job(self, results, job, fid):
        try:
            with open(job.stdout) as f:
                results[fid] = dict((r[0], r[1].strip()) for r in map(lambda l: l.split('\t'), f) if len(r) == 2)
        except Exception as e:
            self.fail("failed to process job output (%s): %s", job.stdout, str(e))

