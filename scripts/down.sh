#!/bin/bash

pkill -f golang/bin/gateway
pkill -f golang/bin/cache
pkill -f golang/bin/run
pkill -f golang/bin/write
pkill -f golang/bin/read
pkill -f golang/bin/config
pkill -f golang/bin/redis
echo "Sanomaru Down!"
