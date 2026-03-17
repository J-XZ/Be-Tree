#!/usr/bin/fish

make
rm -rf tmpdir
mkdir tmpdir
./test -d tmpdir -m benchmark-upserts -t 1000000