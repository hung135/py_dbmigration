#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt
sudo yum install git-tools -y
git config --global credential.helper store
sudo pip install -r requirements.txt
docker pull postgres
docker stop docker-postgres || true && docker rm docker-postgres -f || true

docker run --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
#sleep 2 && docker run -it --rm -e "PGPASSWORD=docker" --link docker-postgres:postgres postgres psql -h postgres -U postgres

#pgadmin setup
/etc/pki/tls/certs/make-dummy-cert dummy_cert.txt
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

#documentation
#https://docs.c9.io/docs/run-an-application
#head -n 29 dummy_cert.txt>certificate.key
#tail -n 26 dummy_cert.txt>certificate.cert
# docker run -p 8080:443 \
# --name docker-pgadmin4 \
# -v "/private/var/lib/pgadmin:/var/lib/pgadmin" \
# -v "$(pwd)/certificate.cert:/certs/server.cert" \
# -v "$(pwd)/certificate.key:/certs/server.key" \
# -e "PGADMIN_DEFAULT_EMAIL=postgres" \
# -e "PGADMIN_DEFAULT_PASSWORD=docker" \
# -e "PGADMIN_ENABLE_TLS=True" \
# -d dpage/pgadmin4

echo https://$(curl http://169.254.169.254/latest/meta-data/instance-id).vfs.cloud9.us-east-2.amazonaws.com >index.html
echo https://$(curl http://169.254.169.254/latest/meta-data/public-ipv4) >>index.html
