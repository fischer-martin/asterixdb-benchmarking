#!/usr/bin/bash

DATASET_URL="git@frosch.cosy.sbg.ac.at:datasets/json/smsen.git"
DATASET_PATH="$( dirname $0 )/../../datasets/smsen"

if [ ! -d "$DATASET_PATH" ]; then
    echo "cloning dataset SMSen"
    git clone $DATASET_URL $DATASET_PATH
fi
