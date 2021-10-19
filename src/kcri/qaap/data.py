#!/usr/bin/env python3
#
# kcri.qaap.data
#
#   Defines the data structures shared across the QAAP.
#

import os, enum
from datetime import datetime
from pico.workflow.blackboard import Blackboard

### Constant Enums

class Platform(enum.Enum):
    MISEQ = 'MiSeq'
    NEXTSEQ = 'NextSeq'
    IGNORE = 'ignore'

### QAAPBlackboard class
#
#   Wraps the generic Blackboard with an API that adds getters and putters for
#   data shared between QAAP services, so they don't grab around in random bags
#   of untyped data.
#
#   Note that kcri.shim.base.ServiceExecution adds more logic to this, accessible
#   from executions.  The demarcation line is not entirely clear; in principle
#   QAAPBlackboard should offer all low level accessors for user inputs as these
#   are put there independent of any execution.

class QAAPBlackboard(Blackboard):
    '''Adds to the generic Blackboard getters and putters specific to the shared
       data definitions in the current QAAP.'''

    def __init__(self, verbose=False):
        super().__init__(verbose)

    # QAAP-level methods

    def start_run(self, service, version, user_inputs):
        self.put('qaap/run_info/service', service)
        self.put('qaap/run_info/version', version)
        self.put('qaap/run_info/time/start', datetime.now().isoformat(timespec='seconds'))
        self.put('qaap/user_inputs', user_inputs)

    def end_run(self):
        start_time = datetime.fromisoformat(self.get('qaap/run_info/time/start'))
        end_time = datetime.now()
        self.put('qaap/run_info/time/end', end_time.isoformat(timespec='seconds'))
        self.put('qaap/run_info/time/duration', (end_time - start_time).total_seconds())

    def put_user_input(self, param, value):
        return self.put('qaap/user_inputs/%s' % param, value)

    def get_user_input(self, param, default=None):
        return self.get('qaap/user_inputs/%s' % param, default)

    def put_qaap_output(self, param, value):
        '''Put value as the output at param.'''
        return self.put('qaap/outputs/%s' % param, value)

    def add_qaap_output(self, param, value):
        '''Append value to the list of outputs at param.'''
        return self.append_to('qaap/outputs/%s' % param, value)

    def get_qaap_output(self, param, default=None):
        '''Return the value of output param.'''
        return self.get('qaap/outputs/%s' % param, default)

    def add_warning(self, warning):
        '''Stores a warning on the 'qaap' top level (note: use service.warning instead).'''
        self.append_to('qaap/warnings', warning)

    # Standard methods for QAAP common data

    def put_base_path(self, path):
        '''The absolute base/work/out dir, this is what we run in initially.'''
        self.put_user_input('out_dir', os.path.abspath(path))

    def get_base_path(self):
        '''Return the absolute base/work/out dir, the initial path where we run.'''
        path = self.get_user_input('out_dir')
        if not path:
            raise Exception('base path (out_dir) is not set')
        return path

    def get_inputs_dir(self):
        '''Return the directory with the symlinks to the inputs.'''
        path = os.path.join(self.get_base_path(), 'inputs')
        os.makedirs(path, exist_ok = True)
        return path

    def get_outputs_dir(self):
        '''Return the directory where the symlinks to the outputs go.'''
        path = os.path.join(self.get_base_path(), 'outputs')
        os.makedirs(path, exist_ok = True)
        return path

    def put_reference_path(self, path):
        '''Stores the path to the user provided reference genome.'''
        self.put_user_input('reference', path)

    def get_reference_path(self, default=None):
        return self.get_user_input('reference', default)

    def put_screening_dbs(self, dic):
        '''Store dict of name -> path for screening databases.'''
        self.put_user_input('screening_dbs', dic)

    def get_screening_dbs(self, default=None):
        return self.get_user_input('screening_dbs', default)

    def put_cleaning_dbs(self, lst):
        '''Store list of paths to cleaning databases.'''
        self.put_user_input('cleaning_dbs', lst)

    def get_cleaning_dbs(self, default=None):
        return self.get_user_input('cleaning_dbs', default)

    def put_platform(self, platform):
        return self.put_user_input('platform', platform.value)

    def is_miseq(self):
        return self.get_user_input('platform', None) == Platform.MISEQ.value

    def is_nextseq(self):
        return self.get_user_input('platform', None) == Platform.NEXTSEQ.value

    def is_metagenomic(self):
        return self.get_user_input('metagenomic', False)

    def is_amplicon(self):
        return self.get_user_input('amplicon', False)

    def get_trim_min_q(self):
        return self.get_user_input('tr_q', 20 if self.is_metagenomic() else 10)

    def get_trim_min_l(self):
        return self.get_user_input('tr_l', 48 if self.is_metagenomic() else 36)

    def get_trim_min_o(self):
        return self.get_user_input('tr_o', 6 if self.is_metagenomic() else 5)

    def get_trimmomatic_adapters(self, which):  # which is PE or SE
        bn = self.get_user_input('tr_a')
        if not bn:
            fn = '/usr/src/ext/trimmomatic/adapters/default-%s.fa' % which
        else:
            fn = '%s-%s.fa' % (bn, which)
        if not os.path.isfile(fn):
            raise Exception("trimmomatic adapter file not found: %s" % fn)
        return fn

    # Helpers for creating the inputs and outputs symlinks

    # It would be more convenient to just work with absolute paths everywhere,
    # but this both looks ugly and will not match inside and outside the
    # container.  So the only abspaths we have are where the symlinks in the
    # inputs directory point to input files on the host file system, and
    # we jump through some hoops here to make all other file references
    # relative.  We store them relative to base dir and make them relative
    # to wherever the job that needs inputs runs.

    def make_symlink(self, dst_dir, fn, sn):
        '''Create symlink in dst_dir to fn from sn, appending .gz if fn has .gz,
           return the link path relative to the base directory.'''
        link_fn = os.path.join(dst_dir, sn)
        if fn.endswith('.gz'): link_fn += '.gz'
        if os.path.exists(link_fn): os.unlink(link_fn)
        if not os.path.exists(dst_dir): os.makedirs(dst_dir)
        os.symlink(os.path.relpath(fn, dst_dir), link_fn)
        return os.path.relpath(link_fn, self.get_base_path())

    def symlink_input_pairs(self, dic):
        '''For each key in dic, make symlinks key_R1.fq and key_R2.fq in dst_dir pointing to fq1 and fq2,
           return new dict having the mapping to the symlinks.'''
        inp_dir = self.get_inputs_dir()
        return dict((k,(self.make_symlink(inp_dir, f1, k+'_R1.fq'),
                        self.make_symlink(inp_dir, f2, k+'_R2.fq'))) for k,(f1,f2) in dic.items())

    def symlink_input_files(self, dic, ext):
        '''For each key in dic, make symlink key.ext[.gz] in dst_dir pointing to fn.
           Return dict like dic but with the filenames swapped for the symlinks.'''
        inp_dir = self.get_inputs_dir()
        return dict((k,self.make_symlink(inp_dir, fn, k+ext)) for k,fn in dic.items())

    def symlink_output_quad(self, sub_dir, fid, lst):
        '''For R1,R2,U1,U2 create symlinks in sub_dir under outputs_dir.
           Return quad with the filenames replaced by the symlinks.'''
        out_dir = os.path.join(self.get_outputs_dir(), sub_dir)
        return (
            self.make_symlink(out_dir, lst[0], fid+'_R1.fq') if lst[0] else lst[0],
            self.make_symlink(out_dir, lst[1], fid+'_R2.fq') if lst[1] else lst[1],
            self.make_symlink(out_dir, lst[2], fid+'_U1.fq') if lst[2] else lst[2],
            self.make_symlink(out_dir, lst[3], fid+'_U2.fq') if lst[3] else lst[3] )

    def symlink_output_file(self, sub_dir, fid, fn, ext):
        '''Create symlink fid.ext[.gz] in sub_dir under outputs.
           Return the symlink (relative to base path).'''
        out_dir = os.path.join(self.get_outputs_dir(), sub_dir)
        return self.make_symlink(out_dir, os.path.abspath(fn), fid+ext)

    # Helpers to make links relative to base_dir relative to pwd

    @staticmethod
    def _rel_rec(o,base): # recursive method
        if not o: return o
        if type(o) is str: return os.path.abspath(os.path.join(base,o))
        if type(o) is list: return list(map(lambda i: QAAPBlackboard._rel_rec(i, base), o))
        if type(o) is tuple: return tuple(map(lambda i: QAAPBlackboard._rel_rec(i, base), o))
        if type(o) is dict: return dict((k, QAAPBlackboard._rel_rec(v,base)) for k,v in o.items())
        raise Exception('missed case in _rel_rec: o is %s' % str(type(o)))

    def relativise(self, obj):
        return self._rel_rec(obj, self.get_base_path())

    # Inputs: single_fqs, paired_fqs, fastas, illumina_run_dir

    def put_illumina_run_dir(self, d):
        '''Stores the path to the MiSeq run output when processing a whole run.'''
        self.put_user_input('illumina_run_dir', d)

    def get_illumina_run_dir(self, default=None):
        return self.get_user_input('illumina_run_dir', default)

    def put_input_il_fqs(self, dic):
        '''Store the illumina read pairs under their sample ids, and replace
           paths in dic by the symlinks in inputs pointing at them.'''
        self.put_user_input('il_fqs', self.symlink_input_pairs(dic))

    def get_input_il_fqs(self, default=None):
        '''Return the dict of input pairs, relativising their paths to the pwd.'''
        return self.relativise(self.get_user_input('il_fqs', default))

    def put_input_pe_fqs(self, dic):
        '''Store the read pairs under their sample ids, and replace paths in
           dic by symlinks in the inputs dir pointing to the real files.'''
        self.put_user_input('pe_fqs', self.symlink_input_pairs(dic))

    def get_input_pe_fqs(self, default=None):
        return self.relativise(self.get_user_input('pe_fqs', default))

    def put_input_se_fqs(self, dic):
        '''Store the single reads under their sample ids, and replace paths in
           dic by symlinks in the inputs dir pointing to the real files.'''
        self.put_user_input('se_fqs', self.symlink_input_files(dic, '.fq'))

    def get_input_se_fqs(self, default=None):
        return self.relativise(self.get_user_input('se_fqs', default))

    def put_input_fastas(self, dic):
        '''Store the fasta files under their sample ids, and replace paths in
           dic by symlinks in the inputs dir pointing to the real files.'''
        self.put_user_input('fastas', self.symlink_input_files(dic, '.fa'))

    def get_input_fastas(self, default=None):
        return self.relativise(self.get_user_input('fastas', default))

    # QAAP outputs - Trimmed

    def add_trimmed_pe_quad(self, fid, quad):
        '''Store the trimmed fastq quad (R1, R2, U1, U2), and make relative
           symlinks in the outputs dir pointing at the files.'''
        self.put_qaap_output('trimmed_pe_fqs/%s' % fid, self.symlink_output_quad('trimmed', fid, quad))

    def get_trimmed_pe_quads(self, default=None):
        '''Return the dict with all trimmed pe quads.'''
        return self.get_qaap_output('trimmed_pe_fqs', default)

    def add_trimmed_se_fq(self, fid, fq):
        '''Stores the trimmed se fq for fid and creates symlink under outputs.'''
        self.put_qaap_output('trimmed_se_fqs/%s' % fid, self.symlink_output_file('trimmed', fid, fq, '.fq'))

    def get_trimmed_se_fqs(self, default=None):
        '''Return the dict with all trimmed se fastqs.'''
        return self.get_qaap_output('trimmed_se_fqs', default)

    # QAAP outputs - Cleaned

    def add_cleaned_pe_quad(self, fid, quad):
        '''Store the cleaned fastq PE quad (R1, R2, U1, U2), and make relative
           symlinks in the outputs dir pointing at the files.'''
        self.put_qaap_output('cleaned_pe_fqs/%s' % fid, self.symlink_output_quad('cleaned', fid, quad))

    def get_cleaned_pe_quads(self, default=None):
        '''Return the dict with all cleaned pe quad.'''
        return self.get_qaap_output('cleaned_pe_fqs', default)

    def add_cleaned_se_fq(self, fid, fq):
        '''Stores the cleaned SE fq for fid and creates symlink under outputs.'''
        self.put_qaap_output('cleaned_se_fqs/%s' % fid, self.symlink_output_file('cleaned', fid, fq, '.fq'))

    def get_cleaned_se_fqs(self, default=None):
        '''Return the dict with all cleaned SE fastqs.'''
        return self.get_qaap_output('cleaned_se_fqs', default)

    # QAAP outputs - FASTA (contigs)

    def add_assembled_fasta(self, fid, fa):
        '''Store path to the assembly and make symlinks in the output dir.'''
        self.put_qaap_output('assemblies/%s' % fid, self.symlink_output_file('assembled', fid, fa, '.fa'))

    def get_assembled_fastas(self, default=None):
        '''Return the dict with all assembled contigs files.'''
        return self.get_qaap_output('assemblies', default)

