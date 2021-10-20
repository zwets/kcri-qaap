# KCRI QAAP Test Databases

Obtain the Univec database:

    wget -O univec.fna ftp://ftp.ncbi.nlm.nih.gov/pub/UniVec/UniVec

Index it (using the bowtie2 in the QAAP container):

    ../../bin/qaap-container-run bowtie2-build univec.fna univec 

Or if you have bowtie2 on your system, just:

    bowtie2-build univec.fna univec 

