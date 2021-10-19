#!/bin/sh

LC_ALL="C"

BASE_NAME="$(basename "$0" .sh)"
BASE_DIR="$(realpath "$(dirname "$0")")"

. "$BASE_DIR/functions.sh"

# Pick a directory that works for you here
TEST_DIR="${TEST_DIR:-/hpc/data/genomics/kcri/miseq/MiSeqOutput/150914_M02836_0007_000000000-AH6J0}"
[ -d "$TEST_DIR" ] || { echo "$(basename "$0"): please set TEST_DIR" && exit 1; }

make_output_dir
run_qaap -x FastQC,FastQScreen,ReadsMetrics -v --platform MiSeq -o "$OUTPUT_DIR" "$TEST_DIR"
check_output

