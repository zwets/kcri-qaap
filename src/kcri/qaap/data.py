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

    def end_run(self, state):
        start_time = datetime.fromisoformat(self.get('qaap/run_info/time/start'))
        end_time = datetime.now()
        self.put('qaap/run_info/time/end', end_time.isoformat(timespec='seconds'))
        self.put('qaap/run_info/time/duration', (end_time - start_time).total_seconds())
        self.put('qaap/run_info/status', state)

    def put_user_input(self, param, value):
        return self.put('qaap/user_inputs/%s' % param, value)

    def get_user_input(self, param, default=None):
        return self.get('qaap/user_inputs/%s' % param, default)

    def put_qaap_output(self, param, value):
        return self.put('qaap/outputs/%s' % param, value)

    def get_qaap_output(self, param, default=None):
        return self.get('qaap/outputs/%s' % param, default)

    def add_warning(self, warning):
        '''Stores a warning on the 'qaap' top level (note: use service warning instead).'''
        self.append_to('qaap/warnings', warning)

    # Standard methods for QAAP common data

    def put_db_dir(self, path):
        '''Stores the QAAP services databases dir.'''
        self.put_user_input('db_dir', path)

    def get_db_dir(self):
        '''Retrieve the user_input/db_dir, this must be set.'''
        db_dir = self.get_user_input('db_dir')
        if not db_dir:
            raise Exception("database dir path is not set")
        elif not os.path.isdir(db_dir):
            raise Exception("db dir path is not a directory: %s" % db_dir)
        return os.path.abspath(db_dir)

    # Inputs: single_fqs, paired_fqs, fastas, miseq_run_dir

    def put_miseq_run_dir(self, d):
        '''Stores the path to the MiSeq run output if processing whole run.'''
        self.put_user_input('miseq_run_dir', d)

    def get_miseq_run_dir(self, default=None):
        return self.get_user_input('miseq_run_dir', default)

    def put_single_fqs(self, dic):
        '''Stores the single fastqs dict as its own (pseudo) user input.'''
        self.put_user_input('single_fqs', dic)

    def get_single_fqs(self, default=None):
        return self.get_user_input('single_fqs', default)

    def put_paired_fqs(self, dic):
        '''Stores the paired fastqs dict as its own (pseudo) user input.'''
        self.put_user_input('paired_fqs', dic)

    def get_paired_fqs(self, default=None):
        return self.get_user_input('paired_fqs', default)

    def put_fastas(self, dic):
        '''Stores the fastas dict as its own (pseudo) user input.'''
        self.put_user_input('fastas', dic)

    def get_fastas(self, default=None):
        return self.get_user_input('fastas', default)

    # QAAP outputs

    def add_new_single_fq(self, sid, path):
        '''Adds a produced unpaired fastq file path to the central output.'''
        self.put_qaap_output('single_fqs/' + sid, path)

    def get_new_single_fqs(self, default=None):
        '''Returns dict of single astqs produced by trimming or cleaning.'''
        self.get_qaap_output('single_fqs', default)

    def add_new_paired_fq(self, sid, pair):
        '''Adds a produced fastq file pair's file paths to the central output.'''
        self.put_qaap_output('paired_fqs/' + sid, pair)

    def get_new_paired_fqs(self, default=None):
        '''Returns dict of paired fastqs produced by trimming or cleaning.'''
        self.get_qaap_output('paired_fqs', default)

    # Reference

    def put_reference_path(self, path):
        '''Stores the path to the user provided reference genome.'''
        self.put_user_input('reference', path)

    def get_reference_path(self, default=None):
        return self.get_user_input('reference', default)

