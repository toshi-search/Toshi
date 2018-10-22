#!/usr/bin/env bash

capnp_verion="0.7.0"

curl -O https://capnproto.org/capnproto-c++-${capnp_verion}.tar.gz
tar zxf capnproto-c++-0.6.1.tar.gz
cd capnproto-c++-0.6.1
./configure --prefix=$HOME
make -j6 check
sudo make install
cd ../