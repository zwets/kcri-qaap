#!/usr/bin/env python3
#
# kcri.qaap.shims.Trimmomatic - service shim to the Trimmomatic backend
#

import os, logging, functools, operator
from pico.workflow.executor import Execution
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "Trimmomatic", DEPS_VERSIONS['trimmomatic']


# The Service class
class TrimmomaticShim:
    '''Service shim that executes the backend.'''

    def execute(self, ident, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Execution.'''

        execution = TrimmomaticExecution(SERVICE, VERSION, ident, blackboard, scheduler)

        try:
            pe_fqs = blackboard.get_paired_fqs(dict())
            se_fqs = blackboard.get_single_fqs(dict())

            if not pe_fqs and not se_fqs:
                UserException('no fastq files to process')

            execution.start(pe_fqs, se_fqs)

        # Failing inputs will throw UserException
        except UserException as e:
            execution.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            execution.fail(str(e))

        return execution

# Single execution of the service
class TrimmomaticExecution(MultiJobExecution):
    '''A single execution of the service, returned by execute().
       schedules a job for every fastq file in the fq_dict'''

    def start(self, pe_fqs, se_fqs):
        if self.state == Execution.State.STARTED:

            # Compute job requirements and threads for trimmomatic
            n_fqs = 2 * len(pe_fqs) + len(se_fqs)
            jobs_per_gb = 4  # Assuming 250M per job
            max_cpu = min(self._scheduler.max_cpu, int(self._scheduler.max_mem * jobs_per_gb))  # assuming 250M/job
            thr_per_job = max(1, int(max_cpu / n_fqs))
            inp_size = functools.reduce(operator.add, map(lambda f: os.stat(f).st_size, se_fqs.values()),0)
            inp_size = functools.reduce(operator.add, map(lambda fs: os.stat(fs[0]).st_size + os.stat(fs[1]).st_size, pe_fqs.values()), inp_size)
            spc = max(0.5, inp_size / (len(pe_fqs)+len(se_fqs)) / (1024*1024*1024))

            job_spec = None
            for fid, (fq1, fq2) in pe_fqs.items():
                params = ['PE', '-threads', 2*thr_per_job, '-summary', 'summary.tsv', '-quiet', '-validatePairs', fq1, fq2, '%s_R1.fq' % fid, '%s_U1.fq' % fid, '%s_R2.fq' % fid, '%s_U2.fq' % fid, "ILLUMINACLIP:default-PE.fa:2:30:10:1:true LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:36" ]
                job_spec = JobSpec('trimmomatic', params, 2*thr_per_job, 2/jobs_per_gb, 2*spc, 20*60)
                self.add_job('trimmomatic-pe_%s' % fid, job_spec, '%s/pe/%s' % (self.ident, fid), ('pe', fid))

            for fid, fq in se_fqs.items():
                params = ['SE', '-threads', thr_per_job, '-summary', 'summary.tsv', '-quiet', fq, '%s.fq' % fid, "SPEC" ]
                job_spec = JobSpec('trimmomatic', params, thr_per_job, 1/jobs_per_gb, spc, 10*60)
                self.add_job('trimmomatic-se_%s' % fid, job_spec, '%s/se/%s' % (self.ident, fid), ('se', fid))

    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        pese, fid = udata

        if pese == 'pe':
            results['paired_fqs'][fid] = (job.file_path('%s_R1.fq' % fid), job.file_path('%s_R2.fq' % fid))
        else:
            results['single_fqs'][fid] = job.file_path('%s.fq' % fid)

