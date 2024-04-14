#!/bin/bash

if [[ -z $(which gh) ]]; then
    echo "GitHub CLI not found (see https://cli.github.com/)"
    exit 1
fi

# Derive version number from build script.
__VERSION=$(grep "version :=" build.sbt | sed -r "s/^.*version := \"(.*)\",.*$/\1/")
echo "building ${__VERSION}"

sbt clean "++ 2.13" compile test "++ 3.2" compile test
if [ $? -ne 0 ]; then
    echo "build failed"
    exit 1
fi

sbt "++ 2.13" publish "++ 3.2" publish
if [ $? -ne 0 ]; then
    echo "publish failed"
    exit 1
fi

git tag -a "v${__VERSION}" -m "release ${__VERSION}"

if [ $? -ne 0 ]; then
    echo "tag creation failed"
    exit 1
fi

echo "released ${__VERSION}"

