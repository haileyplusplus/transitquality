#!/bin/zsh
cd ~/projcode/transitquality || exit
uvicorn 'realtime.devserver:app' --host=0.0.0.0 --port=8500

