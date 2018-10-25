#!/usr/bin/env bash

capnp_version="0.6.1"

curl -O "https://capnproto.org/capnproto-c++-${capnp_version}.tar.gz"
tar zxf capnproto-c++-${capnp_version}.tar.gz
cd capnproto-c++-${capnp_version}
./configure --prefix=$HOME
make -j6 check
sudo make install
cd ../
