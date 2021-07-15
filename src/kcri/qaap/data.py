#!/usr/bin/env python3
#
# kcri.qaap.data
#
#   Defines the data structures shared across the QAAP.
#

import os, enum
from datetime import datetime
from pico.workflow.blackboard import Blackboard


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

    def is_metagenomic(self):
        return self.get_user_input('metagenomic', False)

    def is_amplicon(self):
        return self.get_user_input('amplicon', False)

    def get_trim_min_q(self):
        return self.get_user_input('tr_q', 20 if self.is_metagenomic() else 10)

    def get_trim_min_l(self):
        return self.get_user_input('tr_l', 72 if self.is_metagenomic() else 36)

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
        '''Stores the illumina read pairs under their sample id.'''
        self.put_user_input('illumina_fqs', dic)

    def get_input_il_fqs(self, default=None):
        return self.get_user_input('illumina_fqs', default)

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

    def put_output_paired_fq(self, xid, pair):
        '''Sets the produced fastq pair's file paths for xid.'''
        self.put_qaap_output('paired_fqs/%s' % xid, pair)

    def get_output_paired_fqs(self, xid, default=None):
        '''Returns the output paired fastq tuple for xid.'''
        return self.get_qaap_output('paired_fqs/%s' % xid, default)

    def add_output_single_fq(self, xid, path):
        '''Adds a produced unpaired fastq file path to xid's list.'''
        self.put_qaap_output('single_fqs/%s' % xid, path)

    def get_output_single_fqs(self, xid, default=None):
        '''Returns list of single fastqs produced in xid.'''
        return self.get_qaap_output('single_fqs/%s' % xid, default)

    def put_output_fasta(self, xid, path):
        '''Sets the produced FASTA file for xid.'''
        self.put_qaap_output('contigs/%s' % xid, path)

    def get_output_fasta(self, xid, default=None):
        '''Return path of the FASTA produced by xid.'''
        return self.get_qaap_output('contigs/%s' % xid, default)

