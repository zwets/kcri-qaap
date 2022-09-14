#!/usr/bin/env python3
#
# kcri.qaap.shims.FastQScreen - implements the FastQScreenShim
#

import os, logging, tempfile
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

        task = FastQScreenExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

         # Get the task parameters from the blackboard
        try:
            fastqs = task.get_input_fastqs().values() if Services(sid) == Services.FASTQSCREEN else \
                     task.get_cleaned_fastqs().values() if Services(sid) == Services.CLEAN_FASTQSCREEN else \
                     None

            if fastqs is None: raise Exception('unknown ident in FastQScreenShim: %s' % sid.value)
            if not fastqs: raise UserException('no fastq files to process')

            # Compute resources
            n_fq = len(fastqs)
            max_par = int(task._scheduler.max_mem * 4)    # each thread needs 250MB
            cpu = min(task._scheduler.max_cpu, len(fastqs), max_par)
            mem = cpu / 4               # each job 250M
            tim = n_fq / cpu * 60 * 60   # each job at most 60 min

            # Retrieve the screening databases
            dbs = task.get_screening_dbs()

            # Set up parameters
            params = [
                '--conf', '@PLACEHOLDER@',
                '--subset', 0,
                '--outdir', '.',
                '--force',
                '--quiet',
                '--threads', cpu
            ]

            params.extend(fastqs)

            job_spec = JobSpec('fastq_screen', params, cpu, mem, tim)
            task.store_job_spec(job_spec.as_dict())
            task.start(job_spec, dbs)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task


# Single execution of the service
class FastQScreenExecution(ServiceExecution):
    '''A single execution of the service'''

    _job = None

    def start(self, job_spec, dbs):
        if self.state == Task.State.STARTED:

            # Generate the FastQScreen config file with the DB paths
            cfg = tempfile.NamedTemporaryFile(mode='w', delete=False)
            for i,db in self.get_screening_dbs().items():
                print('DATABASE\t%s\t%s' % (i,db), file=cfg)
            cfg.close()

            # Put the path to the config file in the parameters
            self._cfg = cfg.name
            job_spec.args[1] = self._cfg

            self._job = self._scheduler.schedule_job('fastq-screen-%s' % self.sid, job_spec, self.sid)


    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        # Clean up the config file
        os.unlink(job.spec.args[1])

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

