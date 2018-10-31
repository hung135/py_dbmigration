#!/bin/bash

python3 -m virtualenv .env
source .env/bin/activate
pip install -r requirements.txt
docker pull postgres
docker run --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
 
# This line below will log you in to psql
#docker run -it --rm --link docker-postgres:postgres postgres psql -h postgres -U postgres