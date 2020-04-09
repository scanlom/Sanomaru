#!/bin/bash

pkill -f bin/config
pkill -f bin/read
pkill -f bin/utils
pkill -f bin/write
~/golang/bin/config 2>> ~/logs/Sanomaru.log &
~/golang/bin/read 2>> ~/logs/Sanomaru.log &
~/golang/bin/utils 2>> ~/logs/Sanomaru.log &
~/golang/bin/write 2>> ~/logs/Sanomaru.log &
echo "Sanomaru Bounced!"
