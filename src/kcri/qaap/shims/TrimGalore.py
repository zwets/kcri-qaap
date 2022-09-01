#!/usr/bin/env python3
#
# kcri.qaap.shims.TrimGalore - service shim to the TrimGalore backend
#

import os, logging, functools, operator, re
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

        task = TrimGaloreExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

        try:
            pe_fqs = task.get_input_pairs(dict())
            se_fqs = task.get_input_singles(dict())

            if not pe_fqs and not se_fqs:
                UserException('no fastq files to process')

            task.start(pe_fqs, se_fqs)

        # Failing inputs will throw UserException
        except UserException as e:
            task.fail(str(e))

        # Deeper errors additionally dump stack
        except Exception as e:
            logging.exception(e)
            task.fail(str(e))

        return task


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

            # Guesstimate of mem requirement
            pe_mem, se_mem = pe_cores * 0.5, se_cores * 0.25

            # Schedule the pe jobs
            for fid, (fq1, fq2) in pe_fqs.items():
                self.schedule_pe_job(fid, fq1, fq2, pe_cores, pe_cpu, pe_mem)

            # Schedule the se jobs
            for fid, fq in se_fqs.items():
                self.schedule_se_job(fid, fq, se_cores, se_cpu, se_mem)

    def schedule_pe_job(self, fid, fq1, fq2, cores, cpu, mem):

        params = [ '--paired', '--retain_unpaired', '--dont_gzip', '--cores', cores,
            '--nextseq' if self._blackboard.is_nextseq() else '--quality', self.min_q,
            '--length', self.min_l, '--length_1', self.min_l+1, '--length_2', self.min_l+1,
            '--stringency', self.min_o, '--trim-n', '--basename', fid,
            fq1, fq2 ]

        job_spec = JobSpec('trim_galore', params, cpu, mem, 15*60)
        self.add_job_spec('pe/%s' % fid, job_spec.as_dict())
        self.add_job('trim_galore-pe_%s' % fid, job_spec, '%s/pe/%s' % (self.sid,fid), (True,fid))

    def schedule_se_job(self, fid, fq, cores, cpu, mem):

        params = [ '--dont_gzip', '--cores', cores,
            '--nextseq' if self._blackboard.is_nextseq() else '--quality', self.min_q,
            '--length', self.min_l, '--stringency', self.min_o, '--trim-n', '--basename', fid,
            fq ]

        job_spec = JobSpec('trim_galore', params, cpu, mem, 5*60)
        self.add_job_spec('se/%s' % fid, job_spec.as_dict())
        self.add_job('trim_galore-se_%s' % fid, job_spec, '%s/se/%s' % (self.sid,fid), (False,fid))

    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        is_pe, fid = udata

        res = dict()
        res['summary'] = self.collect_summary(job, is_pe, fid)

        if is_pe:
            check_file = lambda f: None if not os.path.exists(f) or os.stat(f).st_size == 0 else f
            fastqs = (
                    job.file_path(fid + '_val_1.fq'),
                    job.file_path(fid + '_val_2.fq'),
                    check_file(job.file_path(fid + '_R1_unpaired_1.fq')),
                    check_file(job.file_path(fid + '_R2_unpaired_2.fq')) )
            res['fastqs'] = fastqs
            self._blackboard.add_trimmed_pe_quad(fid, fastqs)

            bag = results.get('pe', dict())
            bag[fid] = res
            results['pe'] = bag
        else:
            fq = job.file_path(fid + '_trimmed.fq')
            res['fastq'] = fq
            self._blackboard.add_trimmed_se_fq(fid, fq)

        bag = results.get('pe' if is_pe else 'se', dict())  # cater for when there is one already
        bag[fid] = res
        results['pe' if is_pe else 'se'] = bag

    # Output parsing magic below ...

    to_int = lambda x: int(x.replace(',',''))
    to_str = lambda x: str(x)

    PATTERNS = dict({
        'total_reads':   (re.compile('^Total reads processed: +([0-9,]+)$'), to_int),
        'adapter_reads': (re.compile('^Reads with adapters: +([0-9,]+) .*$'), to_int),
        'passing_reads': (re.compile('^Reads written \\(passing filters\\): +([0-9,]+) .*$'), to_int),
        'total_bp':      (re.compile('^Total basepairs processed: +([0-9,]+) bp$'), to_int),
        'trimmed_bp':    (re.compile('^Quality-trimmed: +([0-9,]+) bp .*$'), to_int),
        'passing_bp':    (re.compile('^Total written \\(filtered\\): +([0-9,]+) bp .*$'), to_int),
        'adapter':       (re.compile('^Using (.+) adapter for trimming \(count: [0-9,]+\)\. .*$'), to_str),
        'adapter_seq':   (re.compile('^Adapter sequence: \'([ACTG]+)\'.*$'), to_str),
        # Beware, suddenly the space between : and value is a tab so allow any space 
        'removed_seqs':  (re.compile('^Sequences removed because they became shorter than the length cutoff of [0-9]+ bp:\\s+([0-9,]+) .*$'), to_int),
        'removed_pairs': (re.compile('^Number of sequence pairs removed because at least one read was shorter than the length cutoff \\([0-9]+ bp\\):\\s+([0-9,]+) .*$'), to_int)
        })

    @staticmethod
    def report_filter(l): # note l has the '\n' still on, silly Python
        for k,m,f in [ (k, p.fullmatch(l[:-1]), f) for k,(p,f) in TrimGaloreExecution.PATTERNS.items() ]:
            if m:
                try: return k, f(m.group(1))
                except: return k, '?'
        return None

    def extract_summary(self, fn):
        with open(fn, 'r') as f:
            return dict(filter(None, map(self.report_filter, f)))

    def collect_summary(self, job, is_pe, fid):
        d = dict()
        if is_pe:
            d['%s_R1' % fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-2]) + '_trimming_report.txt'))
            d['%s_R2' % fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-1]) + '_trimming_report.txt'))
            # Move the removed_pairs which TrimGalore writes to R2 a level up
            d['removed_pairs'] = d['%s_R2' % fid].pop('removed_pairs','?')
        else:
            d[fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-1]) + '_trimming_report.txt'))
        return d

