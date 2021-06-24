#!/bin/sh
#
# Indexes the CGE databases in directory "$1".
# Works for both the test and real databases.
#

LC_ALL="C"

# Check usage and set BASE_DIR to full path of target directory
[ $# -eq 1 ] && [ -d "$1" ] && BASE_DIR="$(realpath -e "$1" 2>/dev/null)" || {
    echo "Usage: $(basename "$0") DB_DIR"
    exit 1
}

# Function to blurt an error and abort
err_exit() { echo "$(basename "$0"): $*" >&2; exit 1; }

# Function returns true if any file matching $1 is newer than file $2
any_newer() { [ ! -e "$2" ] || [ -n "$(find . -name "$1" -cnewer "$2" 2>/dev/null || true)" ]; }

#
# TODO: run any bwa indexing
#

exit 0

# vim: sts=4:sw=4:ai:si:et
