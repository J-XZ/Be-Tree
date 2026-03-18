#!/usr/bin/fish

./build.fish
rm -rf tmpdir
mkdir tmpdir
./build/test -d tmpdir -m benchmark-upserts -t 1000000