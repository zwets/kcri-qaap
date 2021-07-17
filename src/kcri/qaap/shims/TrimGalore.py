#!/usr/bin/env python3
#
# kcri.qaap.shims.TrimGalore - service shim to the TrimGalore backend
#

import os, logging, functools, operator
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "TrimGalore", DEPS_VERSIONS['trim-galore']


# The Service class
class TrimGaloreShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        execution = TrimGaloreExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

        try:
            pe_fqs = execution.get_input_pairs(dict())
            se_fqs = execution.get_input_singles(dict())

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
class TrimGaloreExecution(MultiJobExecution):
    '''A single execution of the service, returned by execute().
       schedules a job for every fastq pair and file.'''

    def start(self, pe_fqs, se_fqs):
        if self.state == Task.State.STARTED:

            # Get the trimming params: quality and length cut off
            min_q = self._blackboard.get_trim_min_q()
            min_l = self._blackboard.get_trim_min_l()

            # Compute max requestable resources, but cores in trim galore are weird:
            # --cores n when n>1 really is 3xn+1 (minus 2 if output not gzipped)
            pe_cores, pe_cpu = (4, 12) if 12*len(se_fqs) <= self._scheduler.max_cpu else (1,1)
            se_cores, se_cpu = (2, 8) if 8*len(se_fqs) <= self._scheduler.max_cpu else (1,1)

            # Compute disc requirement per fq
            inp_spc = functools.reduce(operator.add, map(lambda f: os.stat(f[0]).st_size + os.stat(f[1]).st_size, pe_fqs.values()),
                      functools.reduce(operator.add, map(lambda f: os.stat(f).st_size, se_fqs.values()),0))
            spc_per_fq = max(0.5, inp_spc / (2*len(pe_fqs)+len(se_fqs)) / (1024*1024*1024))

            # Schedule the pe jobs
            for fid, (fq1, fq2) in pe_fqs.items():
                self.schedule_pe_job(fid, fq1, fq2, min_q, min_l, pe_cores, pe_cpu, 0.5, 2*spc_per_fq)

            # Schedule the se jobs
            for fid, fq in se_fqs.items():
                self.schedule_se_job(fid, fq, min_q, min_l, se_cores, se_cpu, 0.25, spc_per_fq)

    def schedule_pe_job(self, fid, fq1, fq2, min_q, min_l, cores, cpu, mem, spc):

        params = ['--basename', fid, '--paired', '--retain_unpaired', '--stringency', 3, '--dont_gzip']
        # Instead of quality use '--nextseq' when 2 colour sequencing (NextSeq, NovaSeq)
        params.extend(['--length', min_l, '--quality', min_q, '--cores', cores])
        #params.extend(['--nextseq', min_q])
        #params.extend(['--output_dir', ...])
        params.extend([fq1,fq2])

        job_spec = JobSpec('trim_galore', params, cpu, mem, spc, 10*60)
        self.store_job_spec(job_spec.as_dict())
        self.add_job('trim_galore-pe_%s' % fid, job_spec, '%s/pe/%s' % (self.sid,fid), (True,fid))

    def schedule_se_job(self, fid, fq, min_q, min_l, cores, cpu, mem, spc):

        params = ['--basename', fid, '--stringency', 3, '--dont_gzip']
        # Instead of quality use '--nextseq' when 2 colour sequencing (NextSeq, NovaSeq)
        params.extend(['--length', min_l, '--quality', min_q, '--cores', cores])
        #params.extend(['--output_dir', ...])
        params.append(fq)

        job_spec = JobSpec('trim_galore', params, cpu, mem, spc, 5*60)
        self.add_job_spec('se/%s'%fid, job_spec.as_dict())
        self.add_job('trim_galore-se_%s' % fid, job_spec, '%s/se/%s' % (self.sid,fid), (False,fid))

    @staticmethod
    def parse_line(line):
        l = line.lower().replace(': ',':').replace(' ','_').split(':')
        return (l[0], int(l[1]) if l[1].find('.') == -1 else float(l[1]))
    
    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        is_pe, fid = udata

        result = dict()

        with open(job.file_path('report.txt'), 'r') as f:
            result['summary'] = dict(map(self.parse_line, f))

        if is_pe:
            bag = results.get('pe', dict())
            result['paired'] = (job.file_path(fid + '_val_1.fq'), job.file_path(fid + '_val_2.fq'))
            result['unpaired'] = list(filter(lambda f: os.stat(f).st_size != 0, [job.file_path(fid + '_unpaired_1.fq'), job.file_path(fid + '_unapaired_2.fq')]))
            bag[fid] = result
            results['pe'] = bag

        else: # len(udata) == 2: # single
            bag = results.get('se', dict())
            result['fastq'] = job.file_path(fid + '_trimmed.fq')
            bag[fid] = result
            results['se'] = bag

