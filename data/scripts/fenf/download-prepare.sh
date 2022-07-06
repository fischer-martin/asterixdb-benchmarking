#!/usr/bin/bash

DATASET_URL="git@frosch.cosy.sbg.ac.at:datasets/json/fenf.git"
DATASET_PATH="$( dirname $0 )/../../datasets/fenf"

if [ ! -d "$DATASET_PATH" ]; then
    echo "cloning dataset FENF"
    git clone $DATASET_URL $DATASET_PATH
fi
