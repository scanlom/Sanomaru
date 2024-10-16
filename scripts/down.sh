#!/bin/bash

pkill -f golang/bin/gateway
pkill -f golang/bin/run
pkill -f golang/bin/write
pkill -f golang/bin/read
pkill -f golang/bin/cache
echo "Sanomaru Down!"
