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
from .shims.Quast import QuastShim
from .shims.ReadsMetrics import ReadsMetricsShim
from .shims.base import UnimplementedService

SERVICES = {
    Services.CONTIGSMETRICS:    ContigsMetricsShim(),
    Services.FASTQC:            FastQCShim(),
    Services.QUAST:             QuastShim(),
    Services.READSMETRICS:      ReadsMetricsShim(),
}

# Check that every enum that is defined has a mapping to a service
for s in Services:
    assert s in SERVICES, "No service shim defined for service %s" % s

