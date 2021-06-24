#!/usr/bin/env python3
#
# kcri.qaap.workflow - Defines the QAAP workflow logic.
#
#   This module defines the DEPENDENCIES dict that captures the workflow logic
#   of the current version of the QAAP.  It defines this in term of the primitives
#   offered by the pico.workflow.logic module in package picoline.
#
#   Sibling module .services defines the mapping from the Services enum defined
#   below to the shims that wrap the actual backends.  The sibling module .data
#   defines the QAAP-specific "blackboard" that gets passed between the services.
#

import pico.workflow.logic
from pico.workflow.logic import ALL, ONE, OPT, OIF, SEQ


### Target definitions
#
#   Define the Params.*, Checkpoints.*, Services.*, and UserTargets.* constants
#   for the QAAP.  The classes are subclassed from their synonymous counterparts
#   in the pico.workflow.logic module.

class Params(pico.workflow.logic.Params):
    '''Flags to signal to the Workflow that some input parameter was provided.'''
    READS = 'reads'         # Signals that user has provided fastq files
    CONTIGS = 'contigs'     # Signals that user has provided contigs
    REFERENCE = 'reference' # Signals that user has specified a reference genome

class Checkpoints(pico.workflow.logic.Checkpoints):
    '''Internal targets for other targets to depend on.  Useful when a service
       takes an input that could come either from user or as a service output.'''
    #DUMMY = 'dummy'         # None at the moment, DUMMY out
    pass

class Services(pico.workflow.logic.Services):
    '''Enum that identifies the available services.  Each corresponds to a shim
       (defined in SERVICES below) that performs the input and output wrangling
       and invokes the actual backend.'''
    CONTIGSMETRICS = 'ContigsMetrics'
    READSMETRICS = 'ReadsMetrics'
    QUAST = 'Quast'
    FASTQC = 'FastQC'

class UserTargets(pico.workflow.logic.UserTargets):
    '''Enum defining the targets that the user can request.'''
    DEFAULT = 'DEFAULT'


### Dependency definitions
#
#   This section defines DEPENDENCIES, a dict that maps each target defined above
#   to its dependencies.  See the WorkflowLogic module for the definition of the
#   ALL, ONE, SEQ, OPT, OIF connectors.

DEPENDENCIES = {

    UserTargets.DEFAULT:        ALL( OPT( Services.CONTIGSMETRICS ),
                                     OPT( Services.READSMETRICS ),
                                     OPT( Services.QUAST ),
                                     OPT( Services.FASTQC ) ),
    Services.READSMETRICS:      Params.READS,
    Services.CONTIGSMETRICS:    Params.CONTIGS,
    Services.FASTQC:            Params.READS,
    Services.QUAST:             ALL( Params.CONTIGS, OPT( Params.REFERENCE ) ),
}

# Consistency check on the DEPENDENCIES definitions

for v in Params:
    assert DEPENDENCIES.get(v) is None, "Params cannot have dependencies: %s" % v

for t in [ Checkpoints, Services, UserTargets ]:
    for v in t:
        assert DEPENDENCIES.get(v), "No dependency is defined for %s" % v

