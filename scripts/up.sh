#!/bin/bash

# nohup is necessary as we use this from the vscode deploy task
nohup ~/golang/bin/cache 2>> ~/logs/SanomaruCache.log &
nohup ~/golang/bin/read 2>> ~/logs/SanomaruRead.log &
nohup ~/golang/bin/write 2>> ~/logs/SanomaruWrite.log &
nohup ~/golang/bin/run 2>> ~/logs/SanomaruRun.log &
nohup ~/golang/bin/gateway 2>> ~/logs/SanomaruGateway.log &
echo "Sanomaru Up!"
