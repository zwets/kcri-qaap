#!/usr/bin/env python3
#
# kcri.qaap.shims.ReadsMetrics - service shim to the fastq-stats backend
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS
from ..workflow import Services

# Our service name and current backend version
SERVICE, VERSION = "ReadsMetrics", DEPS_VERSIONS['fastq-utils']

# Resource parameters per job: cpu, memory, disk, run time reqs
MAX_CPU = 2
MAX_MEM = 0.01
MAX_SPC = 0.001
MAX_TIM = 5 * 60


class ReadsMetricsShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = ReadsMetricsExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

        # From here we catch exception and task will FAIL
        try:
            fastqs = task.get_input_fastqs() if Services(sid) == Services.READSMETRICS else \
                     task.get_output_fastqs() if Services(sid) == Services.POST_READSMETRICS else \
                     None

            if fastqs is None: raise Exception('unknown service in ReadsMetricsShim: %s' % sid)
            if not fastqs: raise UserException('no fastq files to process')

            task.start(fastqs)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task

# Single execution of the service
class ReadsMetricsExecution(MultiJobExecution):
    '''The single execution of service sid in workflow execution _xid (None).
       Schedules a job for every fastq file in the xid.'''

    def start(self, fastqs):
        if self.state == Task.State.STARTED:
            for fid, fpath in fastqs.items():

                # We use shell succinctness to cater for either gzipped or plain input
                cmd = "(gzip -dc '%s' 2>/dev/null || cat '%s') | fastq-stats" % (fpath,fpath) 
                job_spec = JobSpec('sh', [ '-c', cmd, 'fastq-stats' ], MAX_CPU, MAX_MEM, MAX_SPC, MAX_TIM)

                # We add the fid as userdata, so we can use it in collect_output
                self.store_job_spec(job_spec.as_dict())
                self.add_job('fastq-stats-%s' % fid, job_spec, '%s/%s' % (self.sid, fid), fid)

    @staticmethod
    def parse_line(line):
        l = line.strip().split('\t')
        return (l[0],
                int(l[1]) if l[0].startswith('n_') else 
                float(l[1]) if l[0].startswith('pct_') else 
                l[1])

    def collect_job(self, results, job, fid):
        with open(job.stdout) as f:
            results[fid] = dict(map(self.parse_line, f))

