#!/usr/bin/bash

DATASET_URL="git@frosch.cosy.sbg.ac.at:datasets/json/smsen.git"
DATASET_PATH="../../datasets/smsen"
DATASET="$DATASET_PATH/smsen.json"

if [ ! -d "$DATASET_PATH" ]; then
    git clone $DATASET_URL $DATASET_PATH
fi
