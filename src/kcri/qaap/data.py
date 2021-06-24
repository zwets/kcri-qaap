#!/usr/bin/env python3
#
# kcri.qaap.data
#
#   Defines the data structures shared across the QAAP.
#

import os, enum
from datetime import datetime
from pico.workflow.blackboard import Blackboard


### Enums
#
#   Define enums for supported sequencing platform, read pairing.

class SeqPlatform(enum.Enum):
    ILLUMINA = 'Illumina'
    NANOPORE = 'Nanopore'
    PACBIO = 'PacBio'

class SeqPairing(enum.Enum):
    PAIRED = 'paired'
    UNPAIRED = 'unpaired'
    MATE_PAIRED = 'mate-paired'

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

    # Sample ID

    def put_sample_id(self, id):
        '''Store id as the sample id in the summary.'''
        self.put('qaap/summary/sample_id', id)

    def get_sample_id(self):
        return self.get('qaap/summary/sample_id', 'unknown')

    # Sequencing specs

    def put_seq_platform(self, platform):
        '''Stores the sequencing platform as its own (pseudo) user input.'''
        assert isinstance(platform, SeqPlatform)
        self.put_user_input('seq_platform', platform.value)

    def get_seq_platform(self, default=None):
        '''Returns the stored platform as SeqPlatform enum value.'''
        s = self.get_user_input('seq_platform')
        return SeqPlatform(s) if s else default

    def put_seq_pairing(self, pairing):
        '''Stores the sequencing pairing as its own (pseudo) user input.'''
        assert isinstance(pairing, SeqPairing)
        self.put_user_input('seq_pairing', pairing.value)

    def get_seq_pairing(self, default=None):
        '''Returns the stored pairing as SeqPairing enum value.'''
        s = self.get_user_input('seq_pairing')
        return SeqPairing(s) if s else default

    # Contigs and reads

    def put_fastq_paths(self, paths):
        '''Stores the fastqs path as its own (pseudo) user input.'''
        self.put_user_input('fastqs', paths)

    def get_fastq_paths(self, default=None):
        return self.get_user_input('fastqs', default)

    def put_contigs_path(self, path):
        '''Stores the contigs path as its own (pseudo) user input.'''
        self.put_user_input('contigs', path)

    def get_contigs_path(self, default=None):
        return self.get_user_input('contigs', default)

    # Reference

    def put_reference_path(self, path):
        '''Stores the path to the user provided reference genome.'''
        self.put_user_input('reference', path)

    def get_reference_path(self, default=None):
        return self.get_user_input('reference', default)

