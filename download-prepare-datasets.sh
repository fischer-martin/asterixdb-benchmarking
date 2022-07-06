#!/bin/bash

DIRNAME=$( dirname $0 )

pushd $DIRNAME > /dev/null

for script in $(find . -name download-prepare\.sh); do
    bash $script
done

popd > /dev/null
