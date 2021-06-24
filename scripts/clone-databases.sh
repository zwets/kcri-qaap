#!/bin/sh
#
# Clones the CGE database repos from the CGE BitBucket.
#

LC_ALL="C"

IDX_SCRIPT="$(realpath "$(dirname "$0")/index-databases.sh")"

# Check usage and set DEST to full path to directory $1
[ $# -eq 1 ] && [ -d "$1" ] && DEST="$(realpath -e "$1" 2>/dev/null)" || {
    echo "Usage: $(basename "$0") DB_DIR"
    exit 1
}

# Write error message and exit
err_exit() { echo "$(basename "$0"): $*" >&2; exit 1; }

#
# TODO: download databases
#

# Index the databases
"$IDX_SCRIPT" "$DEST"

# Done.
exit 0

# vim: sts=4:sw=4:ai:si:et
