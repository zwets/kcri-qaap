# Dockerfile for the KCRI Quality Analysis & Assembly Pipeline (QAAP)
# ======================================================================

# For full reproducibility, pin the package versions installed by apt
# and conda when releasing to production, using 'package=version'.
# The 'apt-get' and 'conda list' commands output the versions in use.


# Load base Docker image
# ----------------------------------------------------------------------

# Use miniconda3:4.9.2 (Python 3.8, channel 'defaults' only)
FROM continuumio/miniconda3:4.9.2


# System dependencies
# ----------------------------------------------------------------------

# Debian packages
# - g++ and the libboost packages for SKESA
# - default-jre for FastQC and Trimmomatic (and the mkdir for it)
# - ldc and cpanminus for FastQ-Screen
# - trf for KneadData

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -qq update --fix-missing && \
    apt-get -qq install apt-utils && \
    dpkg --configure -a && \
    mkdir /usr/share/man/man1 && \
    apt-get -qq install --no-install-recommends \
        make g++ libz-dev \
        default-jre-headless \
        gawk \
        libboost-program-options-dev \
        libboost-iostreams-dev \
        libboost-regex-dev \
        libboost-timer-dev \
        libboost-chrono-dev \
        libboost-system-dev \
        ldc cpanminus \
    && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/*

# Stop container's bash from leaving .bash_histories everywhere
# and add convenience aliases for interactive (debugging) use
RUN echo "unset HISTFILE" >>/etc/bash.bashrc && \
    echo "alias ls='ls --color=auto' l='ls -CF' la='l -a' ll='l -l' lla='ll -a'" >>/etc/bash.bashrc


# Python dependencies
# ----------------------------------------------------------------------

# Python dependencies via Conda:
# - Install nomkl to prevent MKL being installed; we don't currently
#   use it, it's huge, and it is non-free (why does Conda pick it?)
# - Picoline requires psutil
# - trf for fastq-screen, is not in Debian

#RUN conda config --add channels bioconda && \
#    conda config --add channels defaults && \
#    conda config --set channel_priority strict && \
RUN conda install \
        nomkl \
        psutil && \
    conda list && \
    conda clean -qy --tarballs

# Python dependencies via pip
# - These are in Conda, but dependency issues when installingg
RUN pip install \
        trf \
        multiqc

# SKESA, BLAST, Quast are available in the 'bioconda' channel, but yield
# myriad dependency conflicts, hence we install them from source.


# Install External Deps
#----------------------------------------------------------------------

# Installation root
RUN mkdir -p /usr/src
WORKDIR /usr/src

# Copy the externals to /usr/src/ext
# Note the .dockerignore filters out a lot
COPY ext ext

# Install BLAST by putting its binaries on the PATH,
# and prevent 2.11.0 phone home bug by opting out
# https://github.com/ncbi/blast_plus_docs/issues/15
ENV PATH=/usr/src/ext/ncbi-blast/bin:$PATH \
    BLAST_USAGE_REPORT=false

# Install uf-stats by putting it on the PATH.
ENV PATH=/usr/src/ext/unfasta:$PATH

# Make and install skesa
RUN cd ext/skesa && \
    make clean && make -f Makefile.nongs && \
    mv skesa /usr/local/bin/ && \
    cd .. && rm -rf skesa

# Install fastq-stats
RUN cd ext/fastq-utils && \
    make clean && make fastq-stats && \
    cp fastq-stats /usr/local/bin/ && \
    cd .. && rm -rf fastq-utils

# Install the picoline module
RUN cd ext/picoline && \
    python3 setup.py install && \
    cd .. && rm -rf picoline


# Install the QAAP code
#----------------------------------------------------------------------

# Copy contents of src into /usr/src
COPY src ./

# Install the KCRI QAAP specific code
RUN python3 setup.py install


# Set up user and workdir
#----------------------------------------------------------------------

# Drop to user nobody (running containers as root is not a good idea)
USER nobody:nogroup

# Change to the mounted workdir as initial PWD
WORKDIR /workdir

# No ENTRYPOINT means that any binary on the PATH in the container can
# be run.  Set CMD so that without arguments, user gets QAAP --help.
CMD ["QAAP", "--help"]

