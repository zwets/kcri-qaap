#!/usr/bin/env python3
#
# filescan.py - helper module to find FASTA and fastq files

import sys, os, gzip, shutil, io, re

# Regex patterns matching FASTA/Q file names and illumina read headers.
# Note the question mark in (.*?) is to make the pattern non-greedy.
GENERAL_PAT = re.compile('^(.*?)(\.f(q|astq|a|as|sa|na|asta))?(\.gz)?$')
ILLUMINA_FILE_PAT = re.compile('^(.*?)_S[0-9]+_L[0-9]+_R[12]_[0-9]+\.fastq\.gz$')
ILLUMINA_READ_PAT = re.compile(r'^@[^:]+:\d+:[^:]+:\d+:\d+:\d+:\d+ [12]:[YN]:\d+:[^:]+$')

def err_exit(msg, *args):
    '''Exit with error message and non-zero code.'''
    print(('QAAP: %s' % msg) % args, file=sys.stderr)
    sys.exit(1)

def is_gzipped(fn):
    '''Return True iff fn is a gzipped file.'''
    with open(fn, 'rb') as f:
        b = f.peek(2)
        return b[:2] == b'\x1f\x8b'

def gunzip_file(src, dst):
    '''Unzip file src to file dst.'''
    with gzip.open(src, mode='rb') as f_in:
        with open(dst, 'wb') as f_out:
             shutil.copyfileobj(f_in, f_out)

def gunzip_or_symlink(src, dst):
    '''If src is gzipped, unzip it, else symlink it to dst.'''
    if is_gzipped(src):
        gunzip(src, dst)
    else:
        os.symlink(src, dst)

def detect_filetype(fn):
    '''Detect whether file is (gzipped) fasta or fastq, or other.'''
    with open(fn, 'rb') as f:
        b = f.peek(2)
        if b[:2] == b'\x1f\x8b':
            b = gzip.GzipFile(fileobj=f).peek(2)[:2]
        c = chr(b[0]) if len(b) > 0 else '\x00'
    return 'fasta' if c == '>' else 'fastq' if c == '@' else 'other'

def is_fasta_file(fn):
    '''True iff fn is a FASTA file.'''
    try: return os.path.isfile(fn) and detect_filetype(fn) == 'fasta'
    except: return False

def is_fastq_file(fn):
    '''True iff fn is a FastQ file.'''
    try: return os.path.isfile(fn) and detect_filetype(fn) == 'fastq'
    except: return False

def is_fastq_pair(fn1, fn2):
    '''True iff fn1 and fn2 differ only in having a 1 vs 2 in their base name,
       following one of R, r, _, -, ., or @.  Should cover most cases.'''
    bn1, bn2 = map(os.path.basename, (fn1, fn2))
    pfx = os.path.commonprefix([bn1, bn2])
    sn1, sn2 = map(lambda s: s[len(pfx):], (bn1, bn2))
    return sn1[0] == '1' and sn2[0] == '2' and sn1[1:] == sn2[1:] and (not pfx or pfx[-1] in 'Rr._-@')

def is_illumina_fastq(fn):
    '''True if either fn matches the illumina pattern, or the header is Illumina-like.'''
    bn = os.path.basename(fn)
    if re.fullmatch(ILLUMINA_FILE_PAT, bn):
        return True
    else:
        with open(fn, 'rb') as f:
            b = f.peek(2)
            buf = io.TextIOWrapper(gzip.GzipFile(fileobj=f) if b[:2] == b'\x1f\x8b' else f)
            return re.match(ILLUMINA_READ_PAT, buf.readline())

def is_illumina_pair(fqs):
    '''True iff the file tuple is a pair of Illumina reads.'''
    return len(fqs) == 2 and all(map(is_illumina_fastq, fqs))

def is_illumina_output_dir(dname):
    '''True iff dname is a proper ilumina run output directory.'''
    return os.path.isdir(os.path.join(dname, 'InterOp')) and \
           os.path.isfile(os.path.join(dname, 'RunInfo.xml')) and \
           os.path.isfile(os.path.join(dname, 'runParameters.xml'))

def iter_fastqs(fns):
    '''Iterates arbitrary list of file names, returns fastq singletons and/or pairs.'''
    prev = None
    for this in sorted(filter(is_fastq_file, fns), key=os.path.basename):
        if prev: # try for pair, if so return tuple prev, this 
            if is_fastq_pair(prev, this):
                yield (prev, this)
                prev = None
            else: # return the previous as singleton, hold this
                yield prev
                prev = this
        else: # hold this to see if next pairs
            prev = this
    if prev: # return the last held as singleton
        yield prev

def make_sample_name(fn):
    '''Return base name for FASTA / singleton FASTQ, stripping extensions,
       and if illumina read, also everything from _S.'''
    bn = os.path.basename(fn)
    mat = re.fullmatch(ILLUMINA_FILE_PAT, bn)
    if not mat: mat = re.fullmatch(GENERAL_PAT, bn)
    return mat.group(1) if mat else bn

def make_pair_name(fqpair):
    '''Return base name for FASTQ pair, stripping extensions and read indicator,
       and if illumina read, also everything from _S.'''
    bn1, bn2 = map(os.path.basename, fqpair)
    mat = re.fullmatch(ILLUMINA_FILE_PAT, bn1)
    if mat:
        return mat.group(1)
    else:
        pfx = os.path.commonprefix([bn1, bn2])
        return pfx.rstrip('R').rstrip('._-@')

def add_to_dict(d, k, v):
    '''Add key, value to dict, erroring out if key already there.'''
    if k in d:
        err_exit('duplicate key: %s for values files %s and %s' % (k, v, d[k]))
    else:
        d[k] = v

# Return all fastq files among list of file names as a three-tuple:
# - illumina_pairs, a dict sample_name -> (file_path_r1, file_path_r2)
# - other_pairs, a dict sample_name -> (file_path_r1, file_path_r2)
# - singles, a dict sample_name -> file_path
def scan_fastqs(fns):
    illums = dict()
    pairs = dict()
    singles = dict()
    for it in iter_fastqs(fns):
        if type(it) == tuple:
            if is_illumina_pair(it):
                add_to_dict(illums, make_pair_name(it), it)
            else:
                add_to_dict(pairs, make_pair_name(it), it)
        else:
            add_to_dict(singles, make_sample_name(it), it)
    return (illums, pairs, singles)

def scan_fastas(fns):
    '''Return dict of fasta files among list of file name fns, keyed by sample name.'''
    fastas = dict()
    for it in filter(is_fasta_file, fns):
        add_to_dict(fastas, make_sample_name(it), os.path.abspath(it))
    return fastas

# Same as scan_fastqs, with fastas appended to the tuple.
# When strict, every file name must be either fasta or fastq
def scan_inputs(fns, strict=False):
    lst = list(fns)  # put in list as fns may be iterator, and we need 2 passes
    if strict: 
        f = next(filter(lambda x: not is_fastq_file(x) and not is_fasta_file(x), lst), None)
        if f: err_exit('invalid input: file is neither FASTA nor fastq: %s', f)
    fqs = scan_fastqs(lst)
    return (fqs[0], fqs[1], fqs[2], scan_fastas(fns))

# Runs scan_inputs over the files in directory dname, see scan_inputs for retval
def find_inputs(dname):
    return scan_inputs(map(lambda de: de.path, filter(lambda de: de.is_file, os.scandir(dname))))

# Checks that path is a proper screen/clean database, returns basename, path or errors out
def check_screen_db(path):
    fn = path.strip()
    ix = fn + '.1.bt2'
    if not os.path.isfile(ix):
        err_exit('invalid screening/cleaning database: no index found: %s (did you bowtie2-build it?)' % ix)
    return os.path.basename(fn), os.path.abspath(fn)

