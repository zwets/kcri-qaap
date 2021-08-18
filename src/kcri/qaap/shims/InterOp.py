#!/usr/bin/env python3
#
# kcri.qaap.shims.InterOp - service shim to the Illumina interop parsers
#

import os, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS
from ..workflow import Services

# Our service name and current backend version
SERVICE, VERSION = "InterOp", DEPS_VERSIONS['interop']


class InterOpShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = InterOpExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

         # Get the task parameters from the blackboard
        try:
            d = blackboard.get_illumina_run_dir()
            if not d: raise UserException('no Illumina run output directory found')

            # Set up job
            cmd = "interop_summary --csv=1 '%s' >summary.csv && interop_index-summary --csv=1 '%s' >index-summary.csv" % (d,d)

            job_spec = JobSpec('sh', [ '-c', cmd, 'interop' ], 1, 1, 10*60)
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
class InterOpExecution(ServiceExecution):
    '''A single execution of the service'''

    _job = None

    def start(self, job_spec):
        if self.state == Task.State.STARTED:
            self._job = self._scheduler.schedule_job('interop', job_spec, self.sid)

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        # TODO: parse outputs (though MultiQC will also do)
        self.store_results(dict(TODO_summary = job.file_path("summary.csv"), TODO_index_summary = job.file_path("index-summary.csv")))

