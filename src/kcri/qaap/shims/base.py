#!/usr/bin/env python3
#
# kcri.qaap.shims.base - base functionality across all service shims
#
#   This module defines ServiceExecution and UnimplementedService.
#

import os
from datetime import datetime
from pico.workflow.executor import Task
from pico.jobcontrol.job import Job


### class UserException
#
#   Exception to raise so that an error message is reported back to the user,
#   without a stack trace being dumped on standard error.

class UserException(Exception):
    def __init__(self, message, *args):
        super().__init__(message % args)


### class ServiceExecution
#
#   Base class for the executions returned by all QAAP Service shims.
#   Implements functionality common across all QAAP service executions.

class ServiceExecution(Task):
    '''Implements a single QAAP service execution, subclass for shims to build on.'''

    _blackboard = None
    _scheduler = None

    def __init__(self, svc_shim, svc_version, sid, xid, blackboard, scheduler):
        '''Construct execution of service sid for workflow execution xid,'''
        super().__init__(sid, xid)
        self._blackboard = blackboard
        self._scheduler = scheduler
        self.put_task_info('id', self.ident)
        self.put_task_info('shim', svc_shim)
        self.put_task_info('version', svc_version)
        self.put_task_info('service', sid)
        self.put_task_info('execution', xid)
        self._transition(Task.State.STARTED)

    # Implementable interface of the execution, to be implemented in subclasses

    def report(self):
        '''Default implentation of Task.report, should work for most tasks.
           Checks the job and calls collect_output() to put job output on blackboard.'''

        # If our outward state is STARTED check the job
        if self.state == Task.State.STARTED:
            if self._job.state == Job.State.COMPLETED:
                self.collect_output(self._job)
                if self.state != Task.State.FAILED:
                    self.done()
            elif self._job.state == Job.State.FAILED:
                self.fail(self._job.error)

        return self.state

    # Low level update routines for subclasses

    def get_run_info(self, path):
        return self._blackboard.get('services/%s/run_info/%s' % (self.ident, path))

    def put_run_info(self, path, value):
        '''Update the run_info for this execution to have value at path.'''
        self._blackboard.put('services/%s/run_info/%s' % (self.ident, path), value)

    def add_warning(self, warning):
        '''Add warning to the list of warnings of the execution.'''
        self._blackboard.append_to('services/%s/%s' % (self.ident, 'warnings'), warning)

    def add_warnings(self, warnings):
        '''Add list of warnings if not empty to the list of warnings of the execution.'''
        self.add_warning(list(filter(None, warnings)))  # append_to deals with lists

    def add_error(self, errmsg):
        '''Add errmsg to the list of errors of the service.'''
        self._blackboard.append_to('services/%s/%s' % (self.ident, 'errors'), errmsg)

    def store_job_spec(self, jobspec):
        '''Store the service parameters for a one-job service on the blackboard.'''
        self.put_task_info('job', jobspec)

    def store_results(self, result):
        '''Store the service results on the blackboard.'''
        self._blackboard.put('services/%s/results' % self.ident, result)

    # Override Task._transition() to add timestamps and status on blackboard.

    def _transition(self, new_state, error = None):
        '''Extends the superclass _transition to update the blackboard with status,
           errors, and timestamp fields.'''

        # Rely on superclass to set self.state and self.error
        super()._transition(new_state, error)

        # Set the task_info timestamps
        now_time = datetime.now()
        if new_state == Task.State.STARTED:
            self.put_task_info('time/start', now_time.isoformat(timespec='seconds'))
        else:
            start_time = datetime.fromisoformat(self.get_task_info('time/start'))
            self.put_task_info('time/duration', (now_time - start_time).total_seconds())
            self.put_task_info('time/end', now_time.isoformat(timespec='seconds'))

        # Set the task_info status field and error list
        self.put_task_info('status', new_state.value)
        if new_state == Task.State.FAILED:
            self.add_error(self.error)

        return new_state

    # Getters for the shared fields among services;
    # all of these raise an exception unless default is given

    def is_verbose(self):
        '''Return True if the run was requested to be verbose.'''
        return self.get_user_input('verbose', False)

    def get_db_path(self, db_name, default=None):
        '''Return the path to db_name under db_dir, fail if not a dir.'''
        db_path = os.path.join(self._blackboard.get_db_dir(), db_name)
        if not os.path.isdir(db_path):
            raise UserException("database path not found: %s", db_path)
        return db_path

    def get_user_input(self, param, default=None):
        '''Return the user-provided value for param, fail if no default provided.'''
        ret = self._blackboard.get_user_input(param, default)
        if ret is None:
            raise UserException("required user input is missing: %s" % param)
        return ret

    def get_all_user_fastqs(self, default=None):
        '''Return the dict of all user fastqs, with pairs split into two.'''
        ret = dict()
        for k,(f1,f2) in self._blackboard.get_paired_fqs(dict()).items():
            ret[k+"_R1"], ret[k+"_R2"] = f1, f2
        for k,f in self._blackboard.get_single_fqs(dict()).items():
            ret[k+("_U" if k in d else "")] = f
        if not ret and default is None:
            raise UserException("no fastq files were provided")
        return ret if ret else default

    def get_all_new_fastqs(self, default=None):
        '''Analog of get_all_user_fastqs for produced (trimmed or otherwise) fastqs.'''
        ret = dict()
        for k,(f1,f2) in self._blackboard.get_new_paired_qs(dict()).items():
            ret[k+"_R1"], ret[k+"_R2"] = f1, f2
        for k,f in self._blackboard.get_new_single_fqs(dict()).items():
            ret[k+("_U" if k in d else "")] = f
        if not ret and default is None:
            raise UserException("no new fastq files were produced in this run")
        return ret if ret else default

    def get_contigs_path(self, default=None):
        '''Return the path to the user provided contigs, or fail if no default.'''
        ret = self._blackboard.get_contigs_path(default)
        if ret is None:
            raise UserException("no contigs file was provided")
        return ret

    def get_fastqs_or_contigs_paths(self, default=None):
        '''Return the fastqs or else the contigs in a list, or else default or fail.'''
        ret = self._blackboard.get_fastq_paths(self._blackboard.get_contigs_path(default))
        if ret is None:
            raise UserException("no fastq or contigs files were provided")
        return ret if isinstance(ret,list) else [ret]

    def get_reference_path(self, default=None):
        '''Return path to FASTA with the user provided reference or else the established one, or else default.'''
        ret = self._blackboard.get_reference_path(default)
        if ret is None:
            raise UserException("no reference was specified")
        return ret


### class MultiJobExecution
#
#   ServiceExecution specialisation for when the service spawns a number
#   of jobs.  Method add_job adds a job to its collection.
#   Derived classes must override collect_job and optionally cleanup_job,
#   or may override collect_results which will be invoked when all done.

class MultiJobExecution(ServiceExecution):
    '''A single execution of a service that spawn a number of jobs.'''

    _jobs = None

    def __init__(self, svc_shim, svc_version, sid, xid, blackboard, scheduler):
        '''Construct execution with identity, blackboard and scheduler,
           passes rest on to super().'''
        super().__init__(svc_shim, svc_version, sid, xid, blackboard, scheduler)
        self._jobs = list()

    def add_job(self, jid, jspec, jdir, userdata = None):
        '''Schedules a job with unique job id, spec, work dir, and
           optional arbitrary userdata that will be kept in the job
           table and passed to collect_job and cleanup_job.'''
        job = self._scheduler.schedule_job(jid, jspec, jdir)
        self._jobs.append((job, userdata))

    def report(self):
        '''Implements super callback to report current execution state.
           Default implementation returns until all jobs are done, then
           calls collect_results with the list of (job,userdata) tuples.'''

        # If our outward state is STARTED check the jobs
        if self.state == Task.State.STARTED:

            # We report only once all our jobs are done
            if all(j[0].state in [ Job.State.COMPLETED, Job.State.FAILED ] for j in self._jobs):

                # Invoke virtual method collect_result and store on execution
                self.store_results(self.collect_results(self._jobs))

                # State may have failed in the collect_results step
                if self.state != Task.State.FAILED:

                    # Task state is FAILED if all jobs failed, else COMPLETED
                    if any(j[0].state == Job.State.COMPLETED for j in self._jobs):
                        self.done()
                    else:
                        self.fail('no successful %s job' % self.ident)

        # Return current state of execution as a whole
        return self.state

    def collect_results(self, jobs):
        '''Default implementation, collects output in a dict by invoking
           collect_job for each COMPLETED job in turn, and adding an
           execution error for each FAILED job.  Returns results, so
           that an override could invoke this super, then post process.'''

        results = dict()

        for job, userdata in jobs:

            # Call virtual collect_job for each completed job
            if job.state == Job.State.COMPLETED:
                try:
                    self.collect_job(results, job, userdata)
                except Exception as e:
                    self.fail("failed to collect output for job '%s': %s", job.name, str(e))

            # Add execution error for each failed job
            elif job.state == Job.State.FAILED:
                self.add_error('%s: %s' % (job.name, job.error))

            # Invoke virtual cleanup_job in case job held e.g. temp files
            self.cleanup_job(job, userdata)

        # Return the results object so that subclass can wrap this and postprocess
        return results

    def collect_job(self, results, job, userdata):
        '''Must be overridden if default implementation of collect_results used.
           Should process job output and plug it into the results dictionary.
           Uncaught exceptions will result in whole execution fail.'''
        raise Exception('collect_output not implemented')

    def cleanup_job(self, job, userdata):
        '''Can be overridden if default implementation of collect_results is used
           and jobs need to clean up after themselves (e.g. temp dirs).'''
        pass


### class UnimplementedService
#
#   Shim for services in the SERVICES map that don't have a shim yet.
#   The UnimplementedService returns a ServiceExecution that fails.

class UnimplementedService():
    '''Base unimplemented class, starts but then fails on first report.'''

    def execute(self, sid, xid, blackboard, scheduler):
        return UnimplementedService.Execution( \
                'unimplemented', '1.0.0', sid, xid, blackboard, scheduler)

    class Execution(ServiceExecution):
        def report(self):
            return self.fail("service %s is not implemented", self.ident)

