#!/usr/bin/env python3
#
# kcri.qaap.shims.KneadData - service shim to the KneadData backend
#

import os, logging, functools, operator, re
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import MultiJobExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "KneadData", DEPS_VERSIONS['kneaddata']

# Resource parameters per job, see below
MAX_TIM = None

# The Service class
class KneadDataShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = KneadDataExecution(SERVICE, VERSION, sid, xid, blackboard, scheduler)

        try:
            pe_fqs = dict()
            se_fqs = dict()

            for k, (r1,r2,u1,u2) in task.get_trimmed_quads(dict()).items():
                pe_fqs[k] = (os.path.abspath(r1),os.path.abspath(r2))
                if u1: se_fqs[k+'_U1'] = os.path.abspath(u1)
                if u2: se_fqs[k+'_U2'] = os.path.abspath(u2)

            for k, fq in task.get_trimmed_singles(dict()).items():
                se_fqs[k] = os.path.abspath(fq)

            if not pe_fqs and not se_fqs:
                UserException('no trimmed files to process')

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
class KneadDataExecution(MultiJobExecution):
    '''A single execution of the service, returned by execute().
       schedules a job for every fastq pair (and single?).'''

    def start(self, pe_fqs, se_fqs):
        if self.state == Task.State.STARTED:

            # Retrieve the clean databases
            dbs = self.get_cleaning_dbs()

            # Compute max requestable threads
            gb_per_thr = 0.250
            max_thr = min(self._scheduler.max_cpu, int(self._scheduler.max_mem / gb_per_thr))

            # Compute threads to allocate per fq
            n_fqs = 2*len(pe_fqs) + len(se_fqs)
            thr_per_fq = min(4, max(1, int(max_thr / n_fqs)))

            # Schedule the pe jobs
            for fid, (r1,r2) in pe_fqs.items():
                self.schedule_pe_job(fid, r1, r2, dbs, 2*thr_per_fq, 2*gb_per_thr)

            # Schedule the se jobs
            for fid, fq in se_fqs.items():
                self.schedule_se_job(fid, fq, dbs, thr_per_fq, gb_per_thr)

    def schedule_pe_job(self, fid, fq1, fq2, dbs, cpu, mem):

        params = [ '-i1', fq1, '-i2', fq2, '--output-prefix', fid, '-o', '.', '-t', cpu, '--max-memory', '%.1fG' % mem, '--bypass-trim' ]
        if not self._blackboard.get_user_input('cl_t', False): params.append('--bypass-trf')
        for db in dbs: params += [ '-db', os.path.abspath(db) ]

          #'--fastqc', 'fastqc', '--trf', 'trf', '--run-trim-repetitive' ]
          #--output-prefix OUTPUT_PREFIX
          #--run-fastqc-start
          #--run-fastqc-end
          #--run-trim-repetitive
          #--trimmomatic "$TRIMMOMATIC"
          #--trimmomatic-options "$TRIMOPTIONS"
          #--sequencer-source $ADAPTERS
          #--sequencer-source {NexteraPE,TruSeq2,TruSeq3,none}
          #--log "$1/log.log"
          #--bypass-trim
          #--store-temp-output
          #--remove-intermediate-output
          #--cat-final-output
          #--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
          #--log LOG
          #--bowtie2 BOWTIE2_PATH
          #--bowtie2-options BOWTIE2_OPTIONS
          #--decontaminate-pairs {strict,lenient,unpaired}
          #--reorder
          #--serial
          #--bmtagger BMTAGGER_PATH
          #--match MATCH
          #--mismatch MISMATCH
          #--delta DELTA
          #--pm PM
          #--pi PI
          #--minscore MINSCORE
          #--maxperiod MAXPERIOD

        job_spec = JobSpec('kneaddata', params, cpu, mem, MAX_TIM)
        self.add_job_spec('pe/%s' % fid, job_spec.as_dict())
        self.add_job('kneaddata-pe_%s' % fid, job_spec, '%s/pe/%s' % (self.sid,fid), (True,fid))

    def schedule_se_job(self, fid, fq, dbs, cpu, mem):

        params = [ '-un', fq, '--output-prefix', fid, '-o', '.', '-t', cpu, '--max-memory', '%.1fG' % mem, '--bypass-trim' ]
        if not self._blackboard.get_user_input('cl_t', False): params.append('--bypass-trf')
        for db in dbs: params += [ '-db', os.path.abspath(db) ]

        job_spec = JobSpec('kneaddata', params, cpu, mem, MAX_TIM)
        self.add_job_spec('se/%s' % fid, job_spec.as_dict())
        self.add_job('kneaddata-se_%s' % fid, job_spec, '%s/se/%s' % (self.sid,fid), (False,fid))

    def collect_job(self, results, job, udata):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        is_pe, fid = udata

        res = dict()
        res['summary'] = self.collect_summary(job, is_pe, fid)

        nonz_file = lambda f: f if os.path.isfile(f) and os.path.getsize(f) != 0 else None

        # TODO: undo the KneadData mangling of the Illumina fastq headers
        # sed -i -Ee 's,^(@.*)(:N:0:20)#0/(.)$,\1 \3\2,' path-to-fastq

        if is_pe:
            fqs = ( nonz_file(job.file_path(fid + '_paired_1.fastq')),
                    nonz_file(job.file_path(fid + '_paired_2.fastq')),
                    nonz_file(job.file_path(fid + '_unmatched_1.fastq')),
                    nonz_file(job.file_path(fid + '_unmatched_2.fastq')) )

            res['fastqs'] = fqs
            self._blackboard.add_cleaned_pe_quad(fid, fqs)

            bag = results.get('pe', dict())
            bag[fid] = res
            results['pe'] = bag
        else:
            fq = nonz_file(job.file_path(fid + '.fastq'))
            res['fastq'] = fq
            if fq: self._blackboard.add_cleaned_se_fq(fid, fq)

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
        d = dict(TODO_summary = 'TODO')
#        if is_pe:
#            d['%s_R1' % fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-2]) + '_trimming_report.txt'))
#            d['%s_R2' % fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-1]) + '_trimming_report.txt'))
#            # Move the removed_pairs which TrimGalore writes to R2 a level up
#            d['removed_pairs'] = d['%s_R2' % fid].pop('removed_pairs','?')
#        else:
#            d[fid] = self.extract_summary(job.file_path(os.path.basename(job.spec.args[-1]) + '_trimming_report.txt'))
        return d

