#!/usr/bin/env bash

# Clean existing data first
rm -rf source
mlaunch stop --dir source
mlaunch init --dir source --replicaset --sharded 3 --bind_ip 127.0.0.1
