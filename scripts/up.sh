#!/bin/bash

# nohup is necessary as we use this from the vscode deploy task
nohup ~/golang/bin/config 2>> ~/logs/Sanomaru.log &
nohup ~/golang/bin/utils 2>> ~/logs/Sanomaru.log &
nohup ~/golang/bin/read 2>> ~/logs/Sanomaru.log &
nohup ~/golang/bin/write 2>> ~/logs/Sanomaru.log &
sleep 1s # Give config a chance to startup before we start the cache
nohup ~/golang/bin/cache 2>> ~/logs/Sanomaru.log &
echo "Sanomaru Up!"
