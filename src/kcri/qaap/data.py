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
        '''Stores a warning on the 'qaap' top level (note: use service warning instead).'''
        self.append_to('qaap/warnings', warning)

    # Standard methods for QAAP common data

    def put_db_root(self, path):
        '''Stores the QAAP services database root.'''
        self.put_user_input('db_root', path)

    def get_db_root(self):
        '''Retrieve the user_input/db_root.'''
        db_root = self.get_user_input('db_root')
        if not db_root:
            raise Exception("database root path (--db-root) is not set")
        elif not os.path.isdir(db_root):
            raise Exception("database root path is not a directory: %s" % db_root)
        return os.path.abspath(db_root)

    def put_reference_path(self, path):
        '''Stores the path to the user provided reference genome.'''
        self.put_user_input('reference', path)

    def get_reference_path(self, default=None):
        return self.get_user_input('reference', default)

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
            fn = '%s/trimmomatic/%s-%s.fa' % (self.get_db_root(), bn, which)
        if not os.path.isfile(fn):
            raise Exception("trimmomatic adapter file not found: %s" % fn)
        return fn

    # Inputs: single_fqs, paired_fqs, fastas, illumina_run_dir

    def put_illumina_run_dir(self, d):
        '''Stores the path to the MiSeq run output if processing a whole run.'''
        self.put_user_input('illumina_run_dir', d)

    def get_illumina_run_dir(self, default=None):
        return self.get_user_input('illumina_run_dir', default)

    def put_input_il_fqs(self, dic):
        '''Stores the illumina read pairs under their sample id, and setup
           symlinks.'''
        self.put_user_input('il_fqs', dic)

    def get_input_il_fqs(self, default=None):
        return self.get_user_input('il_fqs', default)

    def put_input_pe_fqs(self, dic):
        self.put_user_input('pe_fqs', dic)

    def get_input_pe_fqs(self, default=None):
        return self.get_user_input('pe_fqs', default)

    def put_input_se_fqs(self, dic):
        '''Stores the single fastqs dict as its own (pseudo) user input.'''
        self.put_user_input('se_fqs', dic)

    def get_input_se_fqs(self, default=None):
        return self.get_user_input('se_fqs', default)

    def put_input_fastas(self, dic):
        '''Stores the fastas dict as its own (pseudo) user input.'''
        self.put_user_input('fastas', dic)

    def get_input_fastas(self, default=None):
        return self.get_user_input('fastas', default)

    # QAAP outputs

    def add_trimmed_pe_fqs(self, fid, quad):
        '''Stores the trimmed fastq quad (R1, R2, U1, U2), where U1 or U2 may be None'''
        self.put_qaap_output('trimmed_pe_fqs/%s' % fid, quad)

    def get_trimmed_pe_fqs(self, default=None):
        '''Return the dict with all trimmed pe quads.'''
        return self.get_qaap_output('trimmed_pe_fqs', default)

    def add_trimmed_se_fq(self, fid, fq):
        '''Stores the trimmed se fq for fid.'''
        self.put_qaap_output('trimmed_se_fqs/%s' % fid, fq)

    def get_trimmed_se_fqs(self, default=None):
        '''Return the dict with all trimmed se fastqs.'''
        return self.get_qaap_output('trimmed_se_fqs', default)

    def add_cleaned_pe_fqs(self, fid, quad):
        '''Stores the cleaned fastq quad location (R1, R2, U1, U2), where U1 or U2 may be None.'''
        self.put_qaap_output('cleaned_pe_fqs/%s' % fid, quad)

    def get_cleaned_pe_fqs(self, default=None):
        '''Return the dict with all cleaned pe quad.'''
        return self.get_qaap_output('cleaned_pe_fqs', default)

    def add_cleaned_se_fqs(self, fid, fq):
        '''Stores the cleaned SE fastq location.'''
        self.put_qaap_output('cleaned_se_fqs/%s' % fid, fq)

    def get_cleaned_se_fqs(self, default=None):
        '''Return the dict with all cleaned SE fastqs.'''
        return self.get_qaap_output('cleaned_se_fqs', default)

