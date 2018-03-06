#!/usr/bin/env bash
export MACHINE_NAME="default"



docker-machine start $MACHINE_NAME
docker-machine env
eval $(docker-machine env)
pwd
docker-compose down
docker-compose up
