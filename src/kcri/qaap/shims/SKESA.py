#!/usr/bin/env python3
#
# kcri.qaap.shims.SKESA - service shim to the SKESA backend
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS


# Our service name and current backend version
SERVICE, VERSION = "SKESA", DEPS_VERSIONS['skesa']

# Resource parameters per job, see below
MAX_SPC = 1
MAX_TIM = 15 * 60

# Output file ex work dir
CONTIGS_OUT = 'contigs.fna'

# The Service class
class SKESAShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = SKESAExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

        # Max out the CPU and MEM (per job) but within reasonability
        max_cpu = min(scheduler.max_cpu, 12)
        max_mem = min(int(scheduler.max_mem), 32)

        # Get the task parameters from the blackboard
        try:
            il_fqs = blackboard.get_input_il_fqs()
            if not il_fqs: raise UserException("no Illumina reads to process, skipping SKESA")

            task.start(il_fqs, max_cpu, max_mem)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task

# Single execution of the service
class SKESAExecution(MultiJobExecution):
    '''The single execution of service sid in workflow execution _xid (None).
       Schedules a job for every illumina paired end read in the xid.'''

    def start(self, pairs, max_cpu, max_mem):
        if self.state == Task.State.STARTED:
            for fid,pair in pairs.items():

                params = [
                    '--contigs_out', CONTIGS_OUT,
                    '--cores', max_cpu,
                    '--memory', max_mem,
                    '--reads', '%s,%s' % pair
                ]

                job_spec = JobSpec('skesa', params, max_cpu, max_mem, MAX_SPC, MAX_TIM)
                self.store_job_spec(job_spec.as_dict())
                self.add_job('skesa-%s' % fid, job_spec, '%s/%s' % (self.sid, fid), fid)

    def collect_job(self, results, job, fid):
        '''Collect the output for this job.'''

        contigs_file = job.file_path(CONTIGS_OUT)

        if os.path.isfile(contigs_file):
            results[fid] = dict({ 'contigs_file': contigs_file })
            self._blackboard.add_assembled_fasta(fid, contigs_file)
        else:
            self.fail("backend job produced no output, check: %s", job.file_path(""))

