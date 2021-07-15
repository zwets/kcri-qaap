#!/usr/bin/env python3
#
# kcri.qaap.services - Defines the services used by the QAAP
#
#   This module defines the SERVICES dict that maps each Service.*
#   enum defined in .workflow to a class (called a 'shim') that
#   implements the service.
#

# Import the Services enum
from .workflow import Services

# Import the shim classes that implement each service
from .shims.ContigsMetrics import ContigsMetricsShim
from .shims.FastQC import FastQCShim
from .shims.FastQScreen import FastQScreenShim
from .shims.InterOp import InterOpShim
from .shims.KneadData import KneadDataShim
from .shims.MultiQC import MultiQCShim
from .shims.Quast import QuastShim
from .shims.ReadsMetrics import ReadsMetricsShim
from .shims.SKESA import SKESAShim
from .shims.SPAdes import SPAdesShim
from .shims.Trimmomatic import TrimmomaticShim
from .shims.Unicycler import UnicyclerShim
from .shims.base import UnimplementedService

SERVICES = {
    Services.CONTIGSMETRICS:    ContigsMetricsShim(),
    Services.FASTQC:            FastQCShim(),
    Services.FASTQSCREEN:       FastQScreenShim(),
    Services.INTEROP:           InterOpShim(),
    Services.KNEADDATA:         KneadDataShim(),
    Services.MULTIQC:           MultiQCShim(),
    Services.QUAST:             QuastShim(),
    Services.POST_FASTQC:       FastQCShim(),       # Same as non-post, they
    Services.POST_FASTQSCREEN:  FastQScreenShim(),  # can tell difference by
    Services.POST_READSMETRICS: ReadsMetricsShim(), # the 'sid' parameter
    Services.READSMETRICS:      ReadsMetricsShim(),
    Services.SKESA:             SKESAShim(),
    Services.SPADES:            SPAdesShim(),
    Services.TRIMMOMATIC:       TrimmomaticShim(),
    Services.UNICYCLER:         UnicyclerShim()
}

# Check that every enum that is defined has a mapping to a service
for s in Services:
    assert s in SERVICES, "No service shim defined for service %s" % s

