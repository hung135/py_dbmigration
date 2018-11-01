#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt
sudo yum install git-tools -y
git config --global credential.helper cache
sudo pip install -r requirements.txt
docker pull postgres
docker stop docker-postgres || true && docker rm docker-postgres -f || true

docker run --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
#sleep 2 && docker run -it --rm -e "PGPASSWORD=docker" --link docker-postgres:postgres postgres psql -h postgres -U postgres

#pgadmin setup
docker stop docker-pgadmin4 || true && docker rm docker-pgadmin4 -f || true
docker pull dpage/pgadmin4
docker run \
-p 8080:80 \
--name docker-pgadmin4 \
-e "PGADMIN_DEFAULT_EMAIL=admin@localhost.com" \
-e "PGADMIN_DEFAULT_PASSWORD=docker" \
-e "DEFAULT_USER=postgres" \
--link docker-postgres:postgres \
-d dpage/pgadmin4

 