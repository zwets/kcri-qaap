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

# Parse string which may be UserTarget or Service
def UserTargetOrService(s):
    try: return UserTargets(s)
    except: return Services(s)

# Detect whether file is (gzipped) fasta or fastq, or other
def detect_filetype(fname):
    with open(fname, 'rb') as f:
        b = f.peek(2)
        if b[:2] == b'\x1f\x8b':
            b = gzip.GzipFile(fileobj=f).peek(2)[:2]
        c = chr(b[0]) if len(b) > 0 else '\x00'
    return 'fasta' if c == '>' else 'fastq' if c == '@' else 'other'

# True iff fname is a FASTA file
def is_fasta_file(fname):
    try: return os.path.isfile(fname) and detect_filetype(fname) == 'fasta'
    except: return False

# True iff fname is a FastQ file
def is_fastq_file(fname):
    try: return os.path.isfile(fname) and detect_filetype(fname) == 'fastq'
    except: return False

# True iff file names fn1 and fn2 are a read pair
def is_fastq_pair(fn1, fn2):
    bn1, bn2 = map(os.path.basename, (fn1, fn2))
    pfx = os.path.commonprefix([bn1, bn2])
    sn1, sn2 = map(lambda s: s[len(pfx):], (bn1, bn2))
    return sn1[0] == '1' and sn2[0] == '2' and sn1[1:] == sn2[1:]

# True iff fastq file has Illumina reads, exception if can't be read
def is_illumina_fastq(fname):
    pat = re.compile(r'^@[^:]+:\d+:[^:]+:\d+:\d+:\d+:\d+ [12]:[YN]:\d+:[^:]+$')
    with open(fname, 'rb') as f:
        b = f.peek(2)
        buf = io.TextIOWrapper(gzip.GzipFile(fileobj=f) if b[:2] == b'\x1f\x8b' else f)
        return re.match(pat, buf.readline())

# True iff dname is a proper miseq run output directory
def is_miseq_output_dir(dname):
    return os.path.isfile(os.path.join(dname, 'InterOp')) and \
           os.path.isfile(os.path.join(dname, 'RunInfo.xml')) and \
           os.path.isfile(os.path.join(dname, 'runParameters.xml'))

# Iterates arbitrary list of file names, returns in turn each singleton
# fastq file or tuple of fastq files if they are a pair
def iter_fastqs(fns):
    prev = None
    for this in sorted(filter(is_fastq_file, fns), key=os.path.basename):
        if prev: # try for pair, if so return tuple prev, this 
            if is_fastq_pair(prev, this):
                yield (prev, os.path.abspath(this))
                prev = None
            else: # return the previous as singleton, hold this
                yield prev
                prev = os.path.abspath(this)
        else: # hold this to see if next pairs
            prev = os.path.abspath(this)
    if prev: # return the last held as singleton
        yield prev

# Add key, value to dict, erroring out if key already there
def add_to_dict(d, k, v):
    if k in d:
        err_exit('duplicate name: %s')
    else:
        d[k] = v

# Strip path and extensions of fastq filename to get sample name
def strip_fq(fn):
    bn = os.path.basename(fn)
    mat = re.search(re.compile('(\.f(ast)?q)?(\.gz)?$'), bn)
    return bn[:mat.start()] if mat else bn

# Strip path, read id, ext of fastq filepair to get sample name
def strip_fqs(fq1, fq2):
    bn1, bn2 = map(os.path.basename, (fq1, fq2))
    pfx = os.path.commonprefix([bn1, bn2])
    return pfx.rstrip('R').rstrip('._-@')

# Return (dict of singles, dict of pairs) of all fastq files in fns
def find_fastqs(fns):
    singles = dict()
    pairs = dict()
    for item in iter_fastqs(fns):
        if type(item) == tuple:
            add_to_dict(pairs, strip_fqs(item[0],item[1]), item)
        else:
            add_to_dict(singles, strip_fq(item), item)
    return (singles, pairs)

# Strip path and extensions of fasta filename to get sample name
def strip_fa(fn):
    bn = os.path.basename(fn)
    mat = re.search(re.compile('(\.f(a|sa|as|na|asta))?(\.gz)?$'), bn)
    return bn[:mat.start()] if mat else bn

# Return list of fasta files among the filenames fns
def find_fastas(fns):
    fastas = dict()
    for item in filter(is_fasta_file, fns):
        add_to_dict(fastas, strip_fa(item), item)
    return fastas



# MAIN -------------------------------------------------------------------

def main():
    '''QAAP main program.'''

    parser = argparse.ArgumentParser(
        description="""\
The KCRI Quality Analysis and Assembly Pipeline (QAAP) performs quality
analysis and assembly of sequencer reads.  It executes a workflow of tool
invocations depending on the targets it is given.

The current version of QAAP accepts only paired-end Illumina reads.  These
can be passed in one of three ways:
 - A MiSeq run output directory, and QAAP will know how to find the reads;
   for QC it will also analyse the 'InterOp' data generated by the MiSeq
 - An arbitrary directory, from which it will collect all fastq file pairs
 - A list of fastq files, which it will pair up automatically

QAAP analyses file names and will automatically pair them up, provided the
names differ in exactly one position, where one has a 1 and the other a 2.
It will use the unique part of the file names as the sample identifier.

The actions to be performed are specified using the -t/--targets option.
Each target corresponds to a recipe for a series of service invocations.
When omitted, the DEFAULT target is assumed.

Use -l/--list-targets to see the available targets.  Combine -t DEFAULT
with -x/--exclude to exclude certain targets or services.
Use -s/--list-services to see available services.

If the run or files are metagenomic, pass the -m/--meta option.

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
    group.add_argument('-m', '--meta',     action='store_true', help="specifies that the run is metagenomic")
    group.add_argument('-r', '--ref',      metavar='FASTA', help="path to a reference genome to use in assembly QC (Quast)")
    group.add_argument('-o', '--out-dir',  metavar='PATH', default='.', help="directory to write output to, default PWD (must be relative when dockerised)")
    group.add_argument('-l', '--list-targets',  action='store_true', help="list the available targets")
    group.add_argument('-s', '--list-services', action='store_true', help="list the available services")
    group.add_argument('-d', '--db-dir',  metavar='PATH', default='/databases', help="base path to databases required by some services (leave default when dockerised)")
    group.add_argument('-v', '--verbose', action='store_true', help="write verbose output to stderr")
    group.add_argument('inputs', metavar='DIR_OR_FILES', nargs='*', default=[], help="input directory or list of fastq files")

    # Resource management arguments
    group = parser.add_argument_group('Scheduler parameters')
    group.add_argument('--max-cpus',      metavar='N',   type=int, default=None, help="number of CPUs to allocate (default: all but 2)")
    group.add_argument('--max-mem',       metavar='GB',  type=int, default=None, help="total memory to allocate (default: 90%)")
    group.add_argument('--max-disc',      metavar='GB',  type=int, default=None, help="total disc space to allocate (default: 80%)")
    group.add_argument('--max-time',      metavar='SEC', type=int, default=None, help="maximum overall run time (default: unlimited)")
    group.add_argument('--poll', metavar='SEC', type=int, default=5, help="seconds between backend polls [5]")

    # Service specific arguments
#    group = parser.add_argument_group('Sequencing specs')
#    group.add_argument('--sq-p', metavar='PLATFORM', help='sequencing platform (%s)' % ','.join(v.value for v in SeqPlatform))
#    group.add_argument('--sq-r', metavar='PAIRING', help='pairing of reads (%s; default: %s when two fastqs are passed, unpaired when one)' % (', '.join(v.value for v in SeqPairing), SeqPairing.PAIRED.value))
    group = parser.add_argument_group('Trimming parameters')
    group.add_argument('--tr-q', metavar='Q', default=None, help="minimum moving window Q score to keep (default: 10 regular, 20 metagenomic)")
    group.add_argument('--tr-l', metavar='LEN', default=36, help="minimum length to keep read after trimming (36)")
    group.add_argument('--tr-u', action='store_true', help="unpaired reads after trimming in their own files (default: keep pair even if mate trimmed too short)")
    group = parser.add_argument_group('Quast parameters')
    group.add_argument('--qu-t', metavar='LEN', default=500, help="threshold contig length for Quast (500)")

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

    # Parse and validate inputs into dir or fastqs or contigs
    miseq_dir = None
    single_fqs = dict()
    paired_fqs = dict()
    fastas = dict()
    if len(args.inputs) == 1 and os.path.isdir(args.inputs[0]):
        if is_miseq_output_dir(args.inputs[0]):
            miseq_dir = os.path.abspath(args.inputs[0])
            single_fqs, paired_fqs = find_fastqs(os.scandir(
                os.path.join(miseq_dir,'Data').join('Intensities').join('BaseCalls')))
            if single_fqs:
                err_exit('unpaired fastq files found: %s', str(single_fqs))
        else:
            single_fqs, paired_fqs = find_fastqs(os.scandir(args.inputs[0]))
            fastas = find_fastas(os.scandir(args.inputs[0]))
    elif args.inputs:
        single_fqs, paired_fqs = find_fastqs(args.inputs)
        fastas = find_fastas(os.scandir(args.inputs[0]))

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
    if not (single_fqs or paired_fqs or fastas):
        if not args.list_targets and not args.list_services:
            err_exit('no inputs were provided')
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

    # TEMPORARY
    sys.exit(0)

    # Set up the Workflow execution
    blackboard = QAAPBlackboard(args.verbose)
    blackboard.start_run(SERVICE, VERSION, vars(args))
    blackboard.put_db_dir(db_dir)
    blackboard.put_run_id(run_id)

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
            'r_id': b.get_run_id(),
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
