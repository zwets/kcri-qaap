# KCRI Quality Analysis and Assembly Pipeline (QAAP)


## Introduction

The KCRI Quality Analysis and Assembly Pipeline (QAP) is the standard
pipeline for QC and assembly at Kilimanjaro Clinical Research Institute
(KCRI).

The QAAP orchestrates standard workflows for processing sequencing reads:

 * Reads (FastQC, FastQ-Screen, fastq-stats)
 * Trimming and cleaning (Trimmomatic, Trim Galore, KneadData)
 * Genome assembly (SKESA, SPAdes, metaSPAdes, flye)
 * Assembly QC (Quast, MetaQuast)

The QAAP comes with sensible default settings and standard workflows for
'plain' bacterial and metagenomic runs (currently Illumina only).


#### Examples

Default run on paired end Illumina reads:

    QAAP read_1.fq.gz read_2.fq.gz

Same and assemble:

    QAAP -t DEFAULT,assembly read_1.fq.gz read_2.fq.gz

Run on a metagenomic gut sample, excluding MGA:

    QAAP --meta read_1.fq.gz read_2.fq.gz

Note that when omitted, the `-t/--target` parameter has value `DEFAULT`,
which implies a standard set of steps.

Perform _only_ metrics (by omitting the DEFAULT target):

    QAAP -t metrics read_1.fq.gz read_2.fq.gz assembly.fna

See what targets (values for the `-t` parameter) are available:

    QAAP --list-targets
    -> metrics, reads-qc, trimming, cleaning, post-qc, assembly, ...

> Note how the targets are 'logical' names for what the QAAP must do.
> The QAAP will determine which services to involve, in what order, and
> what alternatives to try if a service fails.

Service parameters can be passed to individual services in the pipeline.
For instance, to change minimum trim length:

    QAAP --trl=50 ...

For an overview of all available parameters, use `--help`:

    QAAP --help


## Installation

The QAAP was developed to run on a moderately high-end Linux workstation.
It is most easily installed in a Docker container, but could also be set
up in a Conda environment.

The installation has two major steps: building the Docker image, and
downloading the databases.


### Installation - Docker Image

Test that Docker is installed

    docker version
    docker run hello-world

Clone and enter this repository

    git clone https://github.com/zwets/kcri-qaap.git
    cd kcri-qaap

Download the dependencies

    ext/update-deps.sh

Build the `kcri-qaap` Docker image

    ./build.sh

    # Or manually do what build.sh does:
    #docker build -t kcri-qaap "." | tee build.log

Smoke test the container

    # Run 'QAAP --help' in the container, using the bin/QAAP wrapper.
    bin/QAAP --help

Run on test data:

    # Test run QAAP on an assembled sample genome
    test/test-01-fq.sh

If the tests above all end with with `[OK]`, you are good to go.


### Installation - FastQ Screen & KneadData databases

Pick a location for the databases, then:

    # For convenience, set this QAAP_DB_DIR in bin/qaap-container-run
    QAAP_DB_DIR=/hpc/data/genomics/qaap/db   # KCRI path, replace by yours

@TODO: how to build database index@


## Usage

For convenience, add the `bin` directory to your `PATH` in `~/.profile`, or
copy or link the `bin/QAAP` script from `~/.local/bin` or `~/bin`, so that
this works:

    QAAP --help

Run a terminal shell in the container:

    qaap-container-run

Run any of the services in the container directly:

    qaap-container-run trimmomatic --help
    qaap-container-run kcst --help


## Development / Upgrades

* To change the backend versions, set the requested versions in
  `ext/deps-versions.config`, then run `ext/update-deps.sh`.

* To upgrade some backend to the latest on master (or some other branch),
  set their requested version to `master`, then run `ext/update-deps.sh`.

* Before committing a release to production, for reproducibility, remember
  to set the actual version in `ext/deps-version.config`, or use `ext/pin-backend-versions.sh`.

* Run tests after upgrading dependencies:

        # Runs the tests we ran above
        test/run-all-tests.sh


#### Citation

For publications please cite the URL <https://github.com/zwets/kcri-qaap>
of this repository, and the citations required by the tools.


#### Licence

Copyright 2021 (c) Marco van Zwetselaar <zwets@kcri.ac.tz>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
