#!/usr/bin/env python3
#
# kcri.qaap.shims.Trimmomatic - service shim to the Trimmomatic backend
#

import os, logging, functools, operator
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "Trimmomatic", DEPS_VERSIONS['trimmomatic']


# The Service class
class TrimmomaticShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        execution = TrimmomaticExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

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
       schedules a job for every fastq pair and file.'''

    def start(self, pe_fqs, se_fqs):
        if self.state == Task.State.STARTED:

            # Get the trimmomatic params
            min_q = self._blackboard.get_trim_min_q()
            min_l = self._blackboard.get_trim_min_l()
            ad_pe = self._blackboard.get_trimmomatic_adapters('PE') if pe_fqs else None
            ad_se = self._blackboard.get_trimmomatic_adapters('SE') if se_fqs else None

            # Compute max requestable resources
            gb_per_thr = 0.25  # Assuming 250M per thread
            max_thr = min(self._scheduler.max_cpu, int(self._scheduler.max_mem / gb_per_thr))

            # Compute threads per fq
            n_fqs = 2*len(pe_fqs) + len(se_fqs)
            thr_per_fq = min(4, max(1, int(max_thr / n_fqs)))

            # Compute disc requirement per fq
            inp_spc = functools.reduce(operator.add, map(lambda f: os.stat(f[0]).st_size + os.stat(f[1]).st_size, pe_fqs.values()), 
                      functools.reduce(operator.add, map(lambda f: os.stat(f).st_size, se_fqs.values()),0))
            spc_per_fq = max(0.5, inp_spc / n_fqs / (1024*1024*1024))

            # Schedule the pe jobs
            for fid, (fq1, fq2) in pe_fqs.items():
                self.schedule_pe_job(fid, fq1, fq2, min_q, min_l, ad_pe, 2*thr_per_fq, 2*gb_per_thr, 2*spc_per_fq)

            # Schedule the se jobs
            for fid, fq in se_fqs.items():
                self.schedule_se_job(fid, fq, min_q, min_l, ad_se, thr_per_fq, gb_per_thr, spc_per_fq)

    def schedule_pe_job(self, fid, fq1, fq2, min_q, min_l, adap, cpu, mem, spc):

        udata = (fid, '%s_R1.fq'%fid, '%s_U1.fq'%fid, '%s_R2.fq'%fid, '%s_U2.fq'%fid)
        params = ['PE', '-threads', cpu, '-summary', 'summary.txt', '-quiet', '-validatePairs' ]
        params.extend([fq1,fq2])
        params.extend(udata[1:])
        params.extend(['ILLUMINACLIP:%s:2:30:10:1:true'%adap,'LEADING:3','TRAILING:3','SLIDINGWINDOW:4:%d'%min_q, 'MINLEN:%d'%min_l])
        job_spec = JobSpec('trimmomatic', params, cpu, mem, spc, 10*60)
        self.store_job_spec(job_spec.as_dict())
        self.add_job('trimmomatic-pe_%s'%fid, job_spec, '%s/pe/%s'%(self.ident,fid), udata)

    def schedule_se_job(self, fid, fq, min_q, min_l, adap, cpu, mem, spc):

        udata = (fid, '%s.fq'%fid)
        params = ['PE', '-threads', cpu, '-summary', 'summary.txt', '-quiet' ]
        params.extend(udata[1])
        params.extend(['ILLUMINACLIP:%s:2:30:10:1:true'%adap,'LEADING:3','TRAILING:3','SLIDINGWINDOW:4:%d','MINLEN:%d'%(min_q, min_l)])
        job_spec = JobSpec('trimmomatic', params, cpu, mem, spc, 5*60)
        self.add_job_spec('se/%s'%fid, job_spec.as_dict())
        self.add_job('trimmomatic-se_%s'%fid, job_spec, '%s/se/%s'%(self.ident,fid), udata)

    @staticmethod
    def parse_line(line):
        l = line.lower().replace(': ',':').replace(' ','_').split(':')
        return (l[0], int(l[1]) if l[1].find('.') == -1 else float(l[1]))
    
    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        fid = udata[0]
        result = dict()

        with open(job.file_path('summary.txt'), 'r') as f:
            result['summary'] = dict(map(self.parse_line, f))

        if len(udata) == 5:     # paired end
            bag = results.get('pe', dict())
            result['paired'] = [job.file_path(udata[1]), job.file_path(udata[3])]
            result['unpaired'] = list(filter(lambda f: os.stat(f).st_size != 0, [job.file_path(udata[2]), job.file_path(udata[4])]))
            bag[fid] = result
            results['pe'] = bag

        else: # len(udata) == 2: # single
            bag = results.get('se', dict())
            result['fastq'] = job.file_path(udata[1])
            bag[fid] = result
            results['se'] = bag

