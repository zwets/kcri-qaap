#!/usr/bin/env python3
#
# kcri.qaap.workflow - Defines the QAAP workflow logic.
#
#   This module defines the DEPENDENCIES dict that captures the workflow logic
#   of the current version of the QAAP.  It defines this in term of the primitives
#   offered by the pico.workflow.logic module in package picoline.
#
#   The workflow defined herein can be 'dry tested' by running the module from
#   the command line:
#
#       # Depends on picoline, which depends on psutil, so either install
#       # those, or point the PYTHONPATH there, before running this:
#       python3 -m kcri.qaap.workflow --help
#
#   Sibling module .services defines the mapping from the Services enum defined
#   below to the shims that wrap the actual backends.
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
    READS = 'reads'             # User has provided fastq files
    FASTA = 'fasta'             # User has provided fasta files
    META = 'meta'               # All data is metagenomic
    MISEQ_READS = 'miseq-reads' # All reads are MiSeq reads
    MISEQ_RUN = 'miseq-run'     # We are analysing a full MiSeq run (miseq_run_dir)

class Checkpoints(pico.workflow.logic.Checkpoints):
    '''Internal targets for other targets to depend on.  Useful when a service
       takes an input that could come either from user or as a service output.'''
    ASSEMBLED = 'assembled'     # Assembly was performed
    CONTIGS = 'contigs'         # Either FASTA was passed or assembly done

class Services(pico.workflow.logic.Services):
    '''Enum that identifies the available services.  Each corresponds to a shim
       (defined in SERVICES below) that performs the input and output wrangling
       and invokes the actual backend.'''
    CONTIGSMETRICS = 'ContigsMetrics'
    FASTQC = 'FastQC'
    FASTQSCREEN = 'FastQScreen'
    INTEROP = 'InterOp'
    KNEADDATA = 'KneadData'
    MULTIQC = 'MultiQC'
    QUAST = 'Quast'
    READSMETRICS = 'ReadsMetrics'
    SKESA = 'SKESA'
    SPADES = 'SPAdes'
    TRIMMOMATIC = 'Trimmomatic'
    UNICYCLER = 'Unicycler'

class UserTargets(pico.workflow.logic.UserTargets):
    '''Enum defining the targets that the user can request.'''
    DEFAULT = 'DEFAULT'
    QC = 'qc'
    READS_QC = 'reads-qc'
    ASSEMBLY_QC = 'assembly-qc'
    SCREEN = 'screen'
    CLEAN = 'clean'
    TRIM = 'trim'
    ASSEMBLE = 'assemble'
    POLISH = 'polish'

class SystemTargets(pico.workflow.logic.UserTargets):
    '''Enum defining targets that the system (not the user) can request.
       We use it to define the MultiQC workflow that always runs at the end.'''
    MULTIQC = 'multi-qc'


### Dependency definitions
#
#   This section defines DEPENDENCIES, a dict that maps each target defined above
#   to its dependencies.  The connectors are defined by picoline as follows:
#   - ALL: succeeds iff all its clauses succeed (in any order), fails as soon as one fails
#   - SEQ: succeeds iff all its clauses succeed in the specified order, fails as soon as first fails
#   - ONE: succeeds if any of its clauses has succeeded, else starts from left
#   - OPT: triggers execution of it clause, but succeeds even if the clause fails
#   - OIF: succeeds iff its clause succeeds, but does not trigger clause execution

DEPENDENCIES = {

    UserTargets.DEFAULT:        UserTargets.QC,
    UserTargets.QC:             ALL( OPT( UserTargets.READS_QC ),
                                     OPT( UserTargets.ASSEMBLY_QC ) ),
    UserTargets.READS_QC:       ALL( OPT( Services.READSMETRICS ),
                                     OPT( Services.FASTQC ),
                                     OPT( Services.FASTQSCREEN ),
                                     OPT( Services.INTEROP ) ),
    UserTargets.ASSEMBLY_QC:    ALL( OPT( Services.CONTIGSMETRICS ),
                                     OPT( Services.QUAST ) ),
    UserTargets.SCREEN:         Services.FASTQSCREEN,
    UserTargets.CLEAN:          SEQ( Services.KNEADDATA, UserTargets.READS_QC ),
    UserTargets.TRIM:           SEQ( Services.TRIMMOMATIC, UserTargets.READS_QC ), 
    UserTargets.ASSEMBLE:       ONE( Services.SKESA, Services.SPADES, Services.UNICYCLER ),
    UserTargets.POLISH:         Services.UNICYCLER,

    SystemTargets.MULTIQC:      Services.MULTIQC,

    Services.CONTIGSMETRICS:	OIF( Checkpoints.CONTIGS ),
    Services.FASTQC:	        Params.READS,
    Services.FASTQSCREEN:	    Params.READS,
    Services.INTEROP:	        Params.MISEQ_RUN,
    Services.KNEADDATA:         ALL( Params.META, Params.READS ),
    Services.MULTIQC:	        ALL(), # No dependencies
    Services.QUAST:	            OIF( Checkpoints.CONTIGS ),
    Services.READSMETRICS:	    Params.READS,
    Services.SKESA:	            ALL( ONE( Params.MISEQ_READS, Params.MISEQ_RUN ), Params.READS ),
    Services.SPADES:	        ALL( Params.READS, Services.TRIMMOMATIC ),
    Services.TRIMMOMATIC:	    Params.READS,
    Services.UNICYCLER:	        Params.READS,

    Checkpoints.CONTIGS:        ONE( Params.FASTA, Checkpoints.ASSEMBLED ),
    Checkpoints.ASSEMBLED:      ONE( Services.SKESA, Services.SPADES, Services.UNICYCLER ),
}

# Consistency check on the DEPENDENCIES definitions

for v in Params:
    assert DEPENDENCIES.get(v) is None, "Params cannot have dependencies: %s" % v

for t in [ Checkpoints, Services, UserTargets, SystemTargets ]:
    for v in t:
        assert DEPENDENCIES.get(v), "No dependency is defined for %s" % v


### Main 
#
#   The main() entry point for 'dry testing' the workflow defined above.
#
#   Invoke this module to get a CLI which 'executes' the workflow without running
#   any backend services.  You can query its runnable services, tell it which have
#   started, completed, or failed, and it will recompute the state, until the
#   workflow as a whole is fulfilled.

if __name__ == '__main__':

    import sys, argparse, functools, operator
    from pico.workflow.logic import Workflow

    def UserTargetOrService(s):
        '''Translate string to either a UserTarget or Service, throw if neither'''
        try: return UserTargets(s)
        except: return Services(s)

    # Parse arguments
    parser = argparse.ArgumentParser(description='''WorkflowLogic Tester''')
    parser.add_argument('-l', '--list', action='store_true', help="list the available params, services, and targets")
    parser.add_argument('-p', '--param', metavar='PARAM', action='append', default=[], help="set PARAM (option may repeat)")
    parser.add_argument('-x', '--exclude', metavar='SVC_OR_TGT', action='append', default=[], help="exclude service or user target (option may repeat)")
    parser.add_argument('-v', '--verbose', action='store_true', help="be more chatty")
    parser.add_argument('targets', metavar='TARGET', nargs='*', default=['DEFAULT'], help="User targets to complete")
    args = parser.parse_args()

    # Functional shorthands for the arg processing
    list_map = lambda f,i: list(map(f,i))
    list_concat = lambda ls : functools.reduce(operator.concat, ls, list())
    comma_split = lambda s: list(map(lambda i: i.strip(), s.split(',')))

    # Parse command-line options into lists of enum values of the proper type,
    # and pass to the Workflow constructor.
    # Note that strings can be converted to Enum using the Enum constructor.
    # We take into account that user may use comma-separated strings and/or repeat
    # options, so we deal with args that may look like: ['resistance', 'mlst,kcst'].
    try:
        w = Workflow(
            DEPENDENCIES,
            list_map(Params, list_concat(list_map(comma_split, args.param))),
            list_map(UserTargets, list_concat(list_map(comma_split, args.targets))),
            list_map(UserTargetOrService, list_concat(list_map(comma_split, args.exclude)))
            )
    except ValueError as e:
        print("Error: you specified an invalid target name: %s" % e, file=sys.stderr)
        sys.exit(1)

    # Handle the --list option by dumping all available params, targets, services
    if args.list:
        print('Params  : %s' % ', '.join(list_map(lambda x: x.value, Params)))
        print('Targets : %s' % ', '.join(list_map(lambda x: x.value, UserTargets)))
        print('Services: %s' % ', '.join(list_map(lambda x: x.value, Services)))
        sys.exit(0)

    # Check that we haven't failed or completed immediately
    if w.status == Workflow.Status.FAILED:
        print('The workflow failed immediately; did you forget to specify params?')
        sys.exit(0)
    elif w.status == Workflow.Status.COMPLETED:
        print('The workflow completed immediately; did you forget to specify targets?')
        sys.exit(0)

    # Print welcome header and prompt
    print('Workflow ready to rock; %d services are runnable (type \'r\' to see).' % len(w.list_runnable()))
    print('? ', end='', flush=True)

    # Iterate until user stops or workflow completes
    for line in sys.stdin:

        # Define a prompt that shows summary of current status
        prompt = lambda: print('\n[ %s | Runnable:%d Started:%d Completed:%d Failed:%d ] ? ' % (
            w.status.value, 
            len(w.list_runnable()), len(w.list_started()), len(w.list_completed()), len(w.list_failed())), 
            end='', flush=True)

        # Parse the input line into cmd and optional svc
        tok = list(map(lambda i: i.strip(), line.split(' ')))
        cmd = tok[0]
        try:
            svc = Services(tok[1]) if len(tok) > 1 else None
        except ValueError:
            print("Not a valid service name: %s" % tok[1])
            prompt()
            continue
 
        # Pretty print a list of enums as comma-sep values
        pprint = lambda l: print(', '.join(map(lambda s: s.value, l)))

        # Handle the commands
        if cmd.startswith('r'):
            pprint(w.list_runnable())
        elif cmd.startswith('s'):
            if svc:
                w.mark_started(svc)
            pprint(w.list_started())
        elif cmd.startswith('c'):
            if svc:
                w.mark_completed(svc)
            pprint(w.list_completed())
        elif cmd.startswith('f'):
            if svc:
                w.mark_failed(svc)
            pprint(w.list_failed())
        elif line.startswith("q"):
            break
        else:
            print("Commands (may be abbreviated): runnable, started [SVC], completed [SVC], failed [SVC], quit")

        # Else prompt for the next command
        prompt()

    # Done
    print('\nWorkflow status: %s' % w.status)
    if w.list_completed():
        print('- Completed : ', ', '.join(map(lambda s: s.value, w.list_completed())))
    if w.list_failed():
        print('- Failed    : ', ', '.join(map(lambda s: s.value, w.list_failed())))
    if w.list_started():
        print('- Started   : ', ', '.join(map(lambda s: s.value, w.list_started())))
    if w.list_runnable():
        print('- Runnable  : ', ', '.join(map(lambda s: s.value, w.list_runnable())))
    print()

