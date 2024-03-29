#!/usr/bin/env python3
#
# kcri.qaap.shims.Quast - service shim to the Quast backend
#

import os, csv, logging
from pico.workflow.executor import Task
from pico.jobcontrol.job import JobSpec, Job
from .base import ServiceExecution, UserException
from .versions import DEPS_VERSIONS

# Our service name and current backend version
SERVICE, VERSION = "Quast", DEPS_VERSIONS['quast']

# Backend resource parameters: cpu, memory, disk, run time reqs
#MAX_CPU = -1 # all
MAX_MEM = 12
MAX_TIM = None


class QuastShim:
    '''Service shim that executes the backend.'''

    def execute(self, sid, _xid, blackboard, scheduler):
        '''Invoked by the executor.  Creates, starts and returns the Task.'''

        task = QuastExecution(SERVICE, VERSION, sid, _xid, blackboard, scheduler)

         # Get the execution parameters from the blackboard
        MAX_CPU = min(scheduler.max_cpu,12)

        # From here run the execution, and FAIL it on exception
        try:
            fastas = task.get_all_fastas()
            if not fastas: raise UserException('no FASTA files to process')

            # Set up Quast parameters (note there are many more)
            params = [
                '--output-dir', '.',
                '--threads', MAX_CPU,
                '--no-sv',
                '--circos',
#                '--gene-finding',
                '--rna-finding',
#                '--conserved-genes-finding',
#                '--silent',
            ]

            # Append the min-contig threshold for analysis
            min_contig = task.get_user_input('qu_t')
            if min_contig:
                params.extend(['--min-contig', min_contig])

            # Append the reference if we have it
            ref = task.get_reference_path('')
            if ref:
                params.extend(['-r', os.path.abspath(ref)])

#            # Append reads if we have them - only when one FASTA?
#            pairs = task.get_paired_fqs(dict())
#            if pairs:
#                params.extend(['--pe1', fastqs[0], '--pe2', fastqs[1]])
#
#                if len(fastqs) == 2:
#                    if task.is_seq_pairing(SeqPairing.PAIRED):
#                elif len(fastqs) == 1:
#                    elif task.is_seq_pairing(SeqPairing.UNPAIRED):
#                        params.extend(['--single', fastqs[0]])
#                    else:
#                        raise Exception("read pairing must be known for Quast with a single FASTQ files")
#                else:
#                    raise Exception("Quast cannot make sense of more than 2 reads files")

            params.append('--labels')
            params.extend(fastas.keys())
            params.extend(fastas.values())

            job_spec = JobSpec('quast.py', params, MAX_CPU, MAX_MEM, MAX_TIM)
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


translate = dict({
    'Assembly'                      : 'sample',
    '# contigs'                     : 'num_ctg',
    'Largest contig'                : 'max_ctg',
    'Total length'                  : 'tot_len',
    'Reference length'              : 'ref_len',
    'Reference GC (%)'              : 'ref_pct_gc',
    '# contigs (>= 0 bp)'           : 'ctg_min_0k',
    '# contigs (>= 1000 bp)'        : 'ctg_min_1k',
    '# contigs (>= 5000 bp)'        : 'ctg_min_5k',
    '# contigs (>= 10000 bp)'       : 'ctg_min_10k',
    '# contigs (>= 25000 bp)'       : 'ctg_min_25k',
    '# contigs (>= 50000 bp)'       : 'ctg_min_50k',
    'Total length (>= 0 bp)'        : 'len_min_0k',
    'Total length (>= 1000 bp)'     : 'len_min_1k',
    'Total length (>= 5000 bp)'     : 'len_min_5k',
    'Total length (>= 10000 bp)'    : 'len_min_10k',
    'Total length (>= 25000 bp)'    : 'len_min_25k',
    'Total length (>= 50000 bp)'    : 'len_min_50k',
    'GC (%)'                        : 'pct_gc',
    'N50'                           : 'n50',
    'NG50'                          : 'ng50',
    'N75'                           : 'n75',
    'NG75'                          : 'ng75',
    'L50'                           : 'l50',
    'LG50'                          : 'lg50',
    'L75'                           : 'l75',
    'LG75'                          : 'lg75',
    '# total reads'                 : 'num_reads',
    '# left'                        : 'reads_fwd',
    '# right'                       : 'reads_rev',
    'Mapped (%)'                    : 'pct_map',
    'Reference mapped (%)'          : 'pct_map_ref',
    'Properly paired (%)'           : 'pct_paired`',
    'Reference properly paired (%)' : 'pct_paired_ref',
    'Avg. coverage depth'           : 'cov_dep',
    'Reference avg. coverage depth' : 'cov_dep_ref',
    'Coverage >= 1x (%)'            : 'pct_cov_1x',
    'Reference coverage >= 1x (%)'  : 'pct_cov_1x_ref',
    '# misassemblies'               : 'num_mis_asm',
    '# misassembled contigs'        : 'ctg_mis_asm',
    'Misassembled contigs length'   : 'len_mis_asm',
    '# local misassemblies'         : 'lcl_mis_asm',
    '# scaffold gap ext. mis.'      : 'gap_ext_mis',
    '# scaffold gap loc. mis.'      : 'gap_loc_mis',
    '# unaligned mis. contigs'      : 'ctg_unal_mis',
    '# unaligned contigs'           : 'ctg_unal',
    'Unaligned length'              : 'len_unal',
    'Genome fraction (%)'           : 'pct_cov',
    'Duplication ratio'             : 'dup_rat',
    '# N\'s per 100 kbp'            : 'nbase_p_100k',
    '# mismatches per 100 kbp'      : 'mismt_p_100k',
    '# indels per 100 kbp'          : 'indel_p_100k',
    'Largest alignment'             : 'max_aln',
    'Total aligned length'          : 'tot_aln',
    'NA50'                          : 'na50',
    'NGA50'                         : 'nga50',
    'NA75'                          : 'na75',
    'NGA75'                         : 'nga75',
    'LA50'                          : 'la50',
    'LGA50'                         : 'lga50',
    'LA75'                          : 'la75',
    'LGA75'                         : 'lga75',
    })

# Single execution of the service
class QuastExecution(ServiceExecution):
    '''A single execution of the Quast service'''

    _job = None

    def start(self, job_spec):
        if self.state == Task.State.STARTED:
            self._job = self._scheduler.schedule_job('quast', job_spec, 'Quast')

    def collect_output(self, job):
        '''Collect the job output and put on blackboard.
           This method is called by super().report() once job is done.'''

        tsv = job.file_path('report.tsv')
        try:
            metrics = dict()

            with open(tsv, newline='') as f:
                reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
                for row in reader:
                    metrics[translate.get(row[0], row[0])] = row[1]

            self.store_results({
                'contig_threshold': self.get_user_input('qu_t'),
                'metrics': metrics,
                'html_report': job.file_path('report.html')})
            
        except Exception as e:
            self.fail("failed to parse output file %s: %s" % (tsv, str(e)))

