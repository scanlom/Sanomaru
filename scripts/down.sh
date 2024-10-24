#!/bin/bash

kill_ka() {
    pkill -f $1
    for i in {1..10}; do
    PID=`pgrep -f $1`
    if [[ -z ${PID} ]]; then
        break
    fi
    if [[ $i == 10 ]]; then
        echo "$1 ${PID} cannot be killed!"
        echo "Forcefully killing ${PID}"
        kill -9 ${PID}
    else
        sleep 1
    fi
    done
}

kill_ka golang/bin/gateway
kill_ka golang/bin/run
kill_ka golang/bin/write
kill_ka golang/bin/read
kill_ka golang/bin/cache
echo "Sanomaru Down!"
