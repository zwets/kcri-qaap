#!/usr/bin/env python3
#
# filescan.py - helper module to find FASTA and fastq files

import sys, os, gzip, io, re

# Exit with error message and non-zero code
def err_exit(msg, *args):
    print(('QAAP: %s' % msg) % args, file=sys.stderr)
    sys.exit(1)

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

# True iff fname has Illumina reads
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

# Iterates arbitrary list of file names, returns fastq singletons and/or pairs
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
        err_exit('duplicate sample name: %s for files %s and %s' % (k, v, d[k]))
    else:
        d[k] = v

# Patterns matching FASTA/Q file names
# Note the question mark in (.*?) makes the pattern non-greedy; (.*) would
# eat up everything including the optional suffixes.
illumina_pat = re.compile('^(.*?)_S[0-9]+_L[0-9]+_R[12]_[0-9]+\.fastq\.gz$')
general_pat = re.compile('^(.*?)(\.f(q|astq|a|as|sa|na|asta))?(\.gz)?$')

# Return base name for FASTA / singleton FASTQ, stripping extensions,
# and if illumina read, also everything from _S 
def make_sample_name(fn):
    bn = os.path.basename(fn)
    mat = re.fullmatch(illumina_pat, bn)
    if not mat: mat = re.fullmatch(general_pat, bn)
    return mat.group(1) if mat else bn

# Return base name for FASTQ pair, stripping extensions and read indicator,
# and if illumina read, also everything from _S
def make_pair_name(fq1, fq2):
    bn1, bn2 = map(os.path.basename, (fq1, fq2))
    mat = re.fullmatch(illumina_pat, bn1)
    if mat:
        return mat.group(1)
    else:
        pfx = os.path.commonprefix([bn1, bn2])
        return pfx.rstrip('R').rstrip('._-@')

# Return all fastq files among list of file names as (singles, pairs), where
# - singles is dict sample_name -> file_path
# - pairs is dict sample_name -> (file_path_r1, file_path_r2)
def scan_fastqs(fns):
    singles = dict()
    pairs = dict()
    for it in iter_fastqs(fns):
        if type(it) == tuple:
            add_to_dict(pairs, make_pair_name(it[0],it[1]), it)
        else:
            add_to_dict(singles, make_sample_name(it), it)
    return (singles, pairs)

# Return dict of fasta files among list of file name fns, keyed by sample name
def scan_fastas(fns):
    fastas = dict()
    for it in filter(is_fasta_file, fns):
        add_to_dict(fastas, make_sample_name(it), it)
    return fastas

# Return tuple (fastas, single_fqs, paired_fqs) for list of file names, where
# - fastas and single_fqs are both dict sample_name -> file_path
# - paired_fqs is dict sample_name -> (file_path_r1, file_path_r2)
# When strict, every file name must be either fasta or fastq
def scan_inputs(fns, strict=False):
    lst = list(fns)  # put in list as fns may be iterator, and we need 2 passes
    if strict: 
        f = next(filter(lambda x: not is_fastq_file(f) and not is_fasta_file(f), lst), None)
        if f: err_exit('invalid input: file is neither FASTA nor fastq: %s', f)
    fqs = scan_fastqs(lst)
    return (scan_fastas(lst), fqs[0], fqs[1])

# Runs scan_inputs over the files in directory dname, see scan_inputs for retval
def find_inputs(dname):
    return scan_inputs(map(lambda de: de.path, filter(lambda de: de.is_file, os.scandir(dname))))

