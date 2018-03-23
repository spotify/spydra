#!/usr/bin/env bash

USAGE="$(basename $0) -b bucket"

[ $# -eq 0 ] && echo "$USAGE" && exit 0

while getopts "b:" opt; do
    case $opt in
        b) bucket="$OPTARG" ;;
        \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
        :) "Option -$OPTARG requires an argument" >&2; exit 1 ;;
    esac
done

[ "z$bucket" = "z" ] && echo $USAGE && exit 1

CONFIG_DIR=/spydra-it-config
CONFIG_FILE=integration-test-config.json
mkdir -p $CONFIG_DIR

echo "{
  \"log_bucket\": \"$bucket\",
  \"region\": \"europe-west1\"
}" > $CONFIG_DIR/$CONFIG_FILE

echo "Using $CONFIG_DIR/$CONFIG_FILE:"
cat $CONFIG_DIR/$CONFIG_FILE

cd /spydra
mvn clean install  -Dinit-action-uri=gs://$bucket/spydra -Dtest-configuration-dir=$CONFIG_DIR
