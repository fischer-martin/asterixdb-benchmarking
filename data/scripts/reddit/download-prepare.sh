#!/usr/bin/bash

DATASET_URL="git@frosch.cosy.sbg.ac.at:datasets/json/reddit.git"
DATASET_PATH="$( dirname $0 )/../../datasets/reddit"

if [ ! -d "$DATASET_PATH" ]; then
    echo "cloning dataset Reddit"
    git clone $DATASET_URL $DATASET_PATH
fi
