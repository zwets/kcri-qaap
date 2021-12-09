#!/bin/sh

LC_ALL="C"

BASE_NAME="$(basename "$0" .sh)"
BASE_DIR="$(realpath "$(dirname "$0")")"

. "$BASE_DIR/functions.sh"

make_output_dir
run_qaap -v -t qc,polish --platform MiSeq --un-m=bold -o "$OUTPUT_DIR" "$BASE_DIR/data"
#run_qaap -v -t polish --platform MiSeq --un-m=bold -o "$OUTPUT_DIR" "$BASE_DIR/data/test_1.fq.gz" "$BASE_DIR/data/test_2.fq.gz"
check_output

