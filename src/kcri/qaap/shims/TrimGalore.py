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

    min_q = None    # Min quality
    min_l = None    # Min length
    min_o = None    # Min adapter overlap (stringency)

    def start(self, pe_fqs, se_fqs):
        if self.state == Task.State.STARTED:

            # Get the trimming params: quality and length cut off
            self.min_q = self._blackboard.get_trim_min_q()
            self.min_l = self._blackboard.get_trim_min_l()
            self.min_o = self._blackboard.get_trim_min_o()

            # Compute max requestable resources, but cores in trim galore are weird:
            # --cores n when n>1 really is 3n+3 (minus 2 if output not gzipped, as for us)
            pe_cores, pe_cpu = (4, 12) if 12*len(pe_fqs) <= self._scheduler.max_cpu else (1,1)
            se_cores, se_cpu = (2, 8) if 8*len(se_fqs) <= self._scheduler.max_cpu else (1,1)

            # Compute disc requirement per fq
            inp_spc = functools.reduce(operator.add, map(lambda f: os.stat(f[0]).st_size + os.stat(f[1]).st_size, pe_fqs.values()), 
                      functools.reduce(operator.add, map(lambda f: os.stat(f).st_size, se_fqs.values()), 0))
            spc_per_fq = max(0.5, inp_spc / (2*len(pe_fqs)+len(se_fqs)) / (1024*1024*1024))

            # Guesstimate of mem requirement
            pe_mem, se_mem = pe_cores * 0.5, se_cores * 0.25

            # Schedule the pe jobs
            for fid, (fq1, fq2) in pe_fqs.items():
                self.schedule_pe_job(fid, fq1, fq2, pe_cores, pe_cpu, pe_mem, 2*spc_per_fq)

            # Schedule the se jobs
            for fid, fq in se_fqs.items():
                self.schedule_se_job(fid, fq, se_cores, se_cpu, se_mem, spc_per_fq)

    def schedule_pe_job(self, fid, fq1, fq2, cores, cpu, mem, spc):

        params = [ '--paired', '--retain_unpaired', '--dont_gzip', '--cores', cores,
            '--nextseq' if self._blackboard.is_nextseq() else '--quality', self.min_q,
            '--length', self.min_l, 
            '--stringency', self.min_o,
            fq1, fq2 ]

        job_spec = JobSpec('trim_galore', params, cpu, mem, spc, 5*60)
        self.add_job_spec('pe/%s' % fid, job_spec.as_dict())
        self.add_job('trim_galore-pe_%s' % fid, job_spec, '%s/pe/%s' % (self.sid,fid), (True,fid))

    def schedule_se_job(self, fid, fq, cores, cpu, mem, spc):

        params = [ '--dont_gzip', '--cores', cores,
            '--nextseq' if self._blackboard.is_nextseq() else '--quality', self.min_q,
            '--length', self.min_l, 
            '--stringency', self.min_o,
            fq ]

        job_spec = JobSpec('trim_galore', params, cpu, mem, spc, 5*60)
        self.add_job_spec('se/%s' % fid, job_spec.as_dict())
        self.add_job('trim_galore-se_%s' % fid, job_spec, '%s/se/%s' % (self.sid,fid), (False,fid))

    @staticmethod
    def report_filter(l):
        trap = dict({
            "Total reads processed:": 'total_reads',
            "Reads with adapters:": 'adapter_reads',
            "Reads written (passing filters):": 'passing_reads',
            "Total basepairs processed:": 'total_bp',
            "Quality-trimmed:": 'trimmed_bp',
            "Total written (filtered):": 'passing_bp',
            "Number of sequence pairs removed ": 'removed_pairs',
            "Sequences removed because ": 'removed_seqs'})

        for k in trap:
            if l.startswith(k):
                return trap[k], l.split(':')[1].strip()
        if l.startswith('Using '):
            return 'adapter', l.split(' ')[1]
        return None
    
    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        is_pe, fid = udata

        result = dict()

        if is_pe:
            with open(job.stderr, 'r') as f:
                d = dict()
                for k, v in filter(None, map(self.report_filter, f)):
                    d[k] = (d[k], v) if k in d else v
                result['summary'] = d
            result['paired'] = (job.file_path(fid + '_R1_val_1.fq'), job.file_path(fid + '_R2_val_2.fq'))
            result['unpaired'] = list(filter(lambda f: os.stat(f).st_size != 0, [job.file_path(fid + '_R1_unpaired_1.fq'), job.file_path(fid + '_R2_unpaired_2.fq')]))
            bag = results.get('pe', dict())
            bag[fid] = result
            results['pe'] = bag
        else:
            with open(job.stderr, 'r') as f:
                result['summary'] = dict(filter(None, map(self.report_filter, f)))
            result['fastq'] = job.file_path(fid + '_trimmed.fq')
            bag = results.get('se', dict())
            bag[fid] = result
            results['se'] = bag

