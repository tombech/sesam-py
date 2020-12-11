#!/usr/bin/env bash

set -e

echo "Testing..."

if [ "$RUNNER_OS" == "Linux" ] || [ "$RUNNER_OS" == "macOS" ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam
fi

if [ "$RUNNER_OS" == "Windows" ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam.exe
fi

if [ -z "$TRAVIS_OS_NAME" ] ; then
    export SESAM_CLIENT=~/bin/sesam-py
fi

$SESAM_CLIENT -h

export NODE_URL=https://datahub-cd9f97d6.sesam.cloud/api
export PUBLIC_CI_TOKEN=$SESAM_TOKEN

pushd tests

for test_dir in *; do
    if [ -d "$test_dir" -a ! -L "$test_dir" ]; then
        cd "$test_dir"
        echo
        echo "Running tests in $test_dir.."
        /bin/bash run_test.sh
        cd ..
    fi
done

popd
