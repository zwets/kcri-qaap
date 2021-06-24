#!/usr/bin/env python3
#
# QAAP.py - main for the KCRI Assembly and Quality Analysis Pipeline
#

import sys, os, argparse, gzip, io, json, re
from pico.workflow.logic import Workflow
from pico.workflow.executor import Executor
from pico.jobcontrol.subproc import SubprocessScheduler
from .data import QAAPBlackboard, SeqPlatform, SeqPairing
from .services import SERVICES
from .workflow import DEPENDENCIES
from .workflow import UserTargets, Services, Params
from . import __version__

# Global variables and defaults
SERVICE, VERSION = "KCRI QAAP", __version__

# Exit with error message and non-zero code
def err_exit(msg, *args):
    print(('QAAP: %s' % msg) % args, file=sys.stderr)
    sys.exit(1)

# Helper to detect whether file is (gzipped) fasta or fastq
def detect_filetype(fname):
    with open(fname, 'rb') as f:
        b = f.peek(2)
        if b[:2] == b'\x1f\x8b':
            b = gzip.GzipFile(fileobj=f).peek(2)[:2]
        c = chr(b[0]) if len(b) > 0 else '\x00'
    return 'fasta' if c == '>' else 'fastq' if c == '@' else 'other'

# Helper to detect whether fastq file has Illumina reads
def is_illumina_reads(fname):
    pat = re.compile(r'^@[^:]+:\d+:[^:]+:\d+:\d+:\d+:\d+ [12]:[YN]:\d+:[^:]+$')
    with open(fname, 'rb') as f:
        b = f.peek(2)
        buf = io.TextIOWrapper(gzip.GzipFile(fileobj=f) if b[:2] == b'\x1f\x8b' else f)
        return re.match(pat, buf.readline())

# Helper to parse string ts which may be UserTarget or Service
def UserTargetOrService(s):
    try: return UserTargets(s)
    except: return Services(s)


def main():
    '''QAAP main program.'''

    parser = argparse.ArgumentParser(
        description="""\
The KCRI Quality Analysis and Assembly Pipeline (QAAP) performs quality
analysis and assembly of sequencer reads by invoking a number of tools.

The actions to be performed are specified using the -t/--targets option.
Use -l/--list-targets to see the available targets.  Combine -t DEFAULT
with -x/--exclude to exclude certain targets or services.
Use -s/--list-services to see available services.

If a requested service depends on the output of another service, then the
dependency will be automatically added.
""",
        epilog="""\
Instead of passing arguments on the command-line, you can put them, one
per line, in a text file and pass this file with @FILENAME.
""",
        fromfile_prefix_chars='@',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    # General arguments
    group = parser.add_argument_group('General parameters')
    group.add_argument('-t', '--targets',  metavar='TARGET[,...]', default='DEFAULT', help="analyses to perform [DEFAULT]")
    group.add_argument('-x', '--exclude',  metavar='TARGET_OR_SERVICE[,...]', help="targets and/or services to exclude from running")
    group.add_argument('-r', '--ref',metavar='FASTA', help="path to a reference genome (relative to PWD when dockerised)")
    group.add_argument('-i', '--id',       metavar='ID', help="identifier to use for the isolate in reports")
    group.add_argument('-o', '--out-dir',  metavar='PATH', default='.', help="directory to write output to, will be created (relative to PWD when dockerised)")
    group.add_argument('-l', '--list-targets',  action='store_true', help="list the available targets")
    group.add_argument('-s', '--list-services', action='store_true', help="list the available services")
    group.add_argument('-d', '--db-dir',  metavar='PATH', default='/databases', help="base path to service databases (leave default when dockerised)")
    group.add_argument('-v', '--verbose',  action='store_true', help="write verbose output to stderr")
    group.add_argument('files', metavar='FILE', nargs='*', default=[], help="input file(s) in optionally gzipped FASTA or fastq format")

    # Resource management arguments
    group = parser.add_argument_group('Scheduler parameters')
    group.add_argument('--max-cpus',      metavar='N',   type=int, default=None, help="number of CPUs to allocate (default: all)")
    group.add_argument('--max-mem',       metavar='GB',  type=int, default=None, help="total memory to allocate (default: all)")
    group.add_argument('--max-disc',      metavar='GB',  type=int, default=None, help="total disc space to allocate (default: all)")
    group.add_argument('--max-time',      metavar='SEC', type=int, default=None, help="maximum overall run time (default: unlimited)")
    group.add_argument('--poll', metavar='SEC', type=int, default=5, help="seconds between backend polls [5]")

    # Service specific arguments
    group = parser.add_argument_group('Sequencing specs')
    group.add_argument('--sq-p', metavar='PLATFORM', help='Sequencing platform (%s)' % ','.join(v.value for v in SeqPlatform))
    group.add_argument('--sq-r', metavar='PAIRING', help='Pairing of reads (%s; default: %s when two fastqs are passed, unpaired when one)' % (', '.join(v.value for v in SeqPairing), SeqPairing.PAIRED.value))
    group = parser.add_argument_group('Quast parameters')
    group.add_argument('--qu-t', metavar='LEN', default=500, help="Threshold contig length for Quast (500)")

    # Perform the parsing
    args = parser.parse_args()

    # Parse targets and translate to workflow arguments
    targets = []
    try:
        targets = list(map(lambda t: UserTargets(t.strip()), args.targets.split(',') if args.targets else []))
    except ValueError as ve:
        err_exit('invalid target: %s (try --list-targets)', ve)

    # Parse excludes and translate to workflow arguments
    excludes = []
    try:
        excludes = list(map(lambda t_or_s: UserTargetOrService(t_or_s.strip()), args.exclude.split(',') if args.exclude else []))
    except ValueError as ve:
        err_exit('invalid exclude: %s (try --list-targets and --list-services)', ve)

    # Parse and validate files into contigs and fastqs list
    contigs = None
    fastqs = list()
    for f in args.files:
        if not os.path.isfile(f):
            err_exit('no such file: %s', f)
        if detect_filetype(f) == 'fasta':
            if contigs:
                err_exit('more than one FASTA file passed: %s', f)
            contigs = os.path.abspath(f)
        elif detect_filetype(f) == 'fastq':
            if len(fastqs) == 2:
                err_exit('more than two fastq files passed: %s', f)
            fastqs.append(os.path.abspath(f))
        else:
            err_exit("file is neither FASTA not fastq: %s" % f)

    # Parse the ref parameter
    reference = None
    if args.ref:
        if not os.path.isfile(args.ref):
            err_exit('no such file: %s', args.ref)
        if detect_filetype(args.ref) != 'fasta':
            err_exit('reference not a FASTA file: %s', args.ref)
        reference = os.path.abspath(args.ref)

    # Parse the --list options
    if args.list_targets:
        print('targets:', ','.join(t.value for t in UserTargets))
    if args.list_services:
        print('services:', ','.join(s.value for s in Services))

    # Exit when no contigs and/or fastqs were provided
    if not contigs and not fastqs:
        if not args.list_targets and not args.list_services:
            err_exit('no input files were provided')
        else:
            sys.exit(0)

    # Check existence of the db_dir directory
    if not os.path.isdir(args.db_dir):
        err_exit('no such directory for --db-dir: %s', args.db_dir)
    db_dir = os.path.abspath(args.db_dir)

    # Now that path handling has been done, and all file references made,
    # we can safely change the base working directory to out-dir.
    try:
        os.makedirs(args.out_dir, exist_ok=True)
        os.chdir(args.out_dir)
    except Exception as e:
        err_exit('error creating or changing to --out-dir %s: %s', args.out_dir, str(e))

    # Generate sample id if not given
    sample_id = args.id
    if not sample_id:
        if contigs and not fastqs:
            _, fname = os.path.split(contigs)
            sample_id, ext = os.path.splitext(fname)
            if ext == '.gz':
                sample_id, _ = os.path.splitext(sample_id)
        elif fastqs:
            # Try if it is Illumina
            pat = re.compile('^(.*)_S[0-9]+_L[0-9]+_R[12]_[0-9]+\.fastq\.gz$')
            _, fname = os.path.split(fastqs[0])
            mat = pat.fullmatch(fname)
            if mat:
                sample_id = mat.group(1)
            else: # no illumina, try to fudge something from common part
                common = os.path.commonprefix(fastqs)
                _, sample_id = os.path.split(common)
                # sample_id now is the common part, chop any _ or _R
                if sample_id[-2:] == "_R" or sample_id[-2:] == "_r":
                    sample_id = sample_id[:-2]
                elif sample_id[-1] == "_":
                    sample_id = sample_id[:-1]
        if not sample_id:
            sample_id = "SAMPLE"

    # Set up the Workflow execution
    blackboard = QAAPBlackboard(args.verbose)
    blackboard.start_run(SERVICE, VERSION, vars(args))
    blackboard.put_db_dir(db_dir)
    blackboard.put_sample_id(sample_id)

    # Set platform and pairing defaults when fastqs are present
    seq_platform = None
    seq_pairing = None

    if fastqs:
        if args.sq_p:
            try: seq_platform = SeqPlatform(args.sq_p)
            except: err_exit('invalid value for --sq-p (sequencing platform): %s', args.sq_p)
        elif all(map(lambda f: is_illumina_reads(f), fastqs)):
            seq_platform = SeqPlatform.ILLUMINA
        else:
            err_exit('please specify sequencing platform (--sq-p)')

        if args.sq_r:
            try: seq_pairing = SeqPairing(args.sq_r)
            except: err_exit('invalid value for --sq-r (read pairing): %s', args.sq_r)
        else:
            seq_pairing = SeqPairing.PAIRED if len(fastqs) == 2 else SeqPairing.UNPAIRED
            blackboard.add_warning('assuming %s reads' % seq_pairing.value)

    # Set the workflow params based on user inputs present
    params = list()
    if contigs:
        params.append(Params.CONTIGS)
        blackboard.put_contigs_path(contigs)
    if fastqs:
        params.append(Params.READS)
        blackboard.put_fastq_paths(fastqs)
        blackboard.put_seq_platform(seq_platform)
        blackboard.put_seq_pairing(seq_pairing)
    if reference:
        params.append(Params.REFERENCE)
        blackboard.put_reference_path(reference)

    # Pass the actual data via the blackboard
    workflow = Workflow(DEPENDENCIES, params, targets, excludes)
    scheduler = SubprocessScheduler(args.max_cpus, args.max_mem, args.max_disc, args.max_time, args.poll, not args.verbose)
    executor = Executor(workflow, SERVICES, scheduler)
    executor.execute(blackboard)
    blackboard.end_run(workflow.status.value)

    # Write the JSON results file
    with open('qaap-results.json', 'w') as f_json:
        json.dump(blackboard.as_dict(args.verbose), f_json)

    # Write the TSV summary results file
    with open('qaap-summary.tsv', 'w') as f_tsv:
        commasep = lambda l: ','.join(l) if l else ''
        b = blackboard
        d = dict({
            's_id': b.get_sample_id(),
            'n_reads': b.get('services/ReadsMetrics/results/n_reads', 'NA'),
            'nt_read': b.get('services/ReadsMetrics/results/n_bases', 'NA'),
            'pct_q30': b.get('services/ReadsMetrics/results/pct_q30', 'NA'),
            'n_ctgs': b.get('services/ContigsMetrics/results/n_seqs', 'NA'),
            'nt_ctgs': b.get('services/ContigsMetrics/results/tot_len', 'NA'),
            'n1':  b.get('services/ContigsMetrics/results/max_len', 'NA'),
            'n50':  b.get('services/ContigsMetrics/results/n50', 'NA'),
            'l50':  b.get('services/ContigsMetrics/results/l50', 'NA'),
            'pct_gc':  b.get('services/ContigsMetrics/results/pct_gc', b.get('services/ReadsMetrics/results/pct_gc', 'NA')),
            })
        print('\t'.join(d.keys()), file=f_tsv)
        print('\t'.join(map(lambda v: v if v else '', d.values())), file=f_tsv)

    # Done done
    return 0


if __name__ == '__main__':
   main()
