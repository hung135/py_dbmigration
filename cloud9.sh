#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt
sudo yum install git-tools -y
git config --global credential.helper cache
sudo pip install -r requirements.txt
docker pull postgres
docker stop docker-postgres || true && docker rm docker-postgres || true
docker run --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
#docker run -it  --rm -e PGPASSWORD=docker --link docker-postgres:postgres postgres psql -h postgres -U postgres

 