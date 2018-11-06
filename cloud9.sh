#!/bin/bash

#python3 -m virtualenv .env
#source .env/bin/activate
#pip install -r requirements.txt
sudo yum install git-tools -y
git config --global credential.helper store
sudo pip install -r requirements.txt
docker pull postgres
docker pull nginx
docker pull dpage/pgadmin4

docker stop $(docker ps -aq)
docker rm $(docker ps -a -q)
#docker system prune -a -f
docker run --name docker-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
#sleep 2 && docker run -it --rm -e "PGPASSWORD=docker" --link docker-postgres:postgres postgres psql -h postgres -U postgres

#pgadmin setup
/etc/pki/tls/certs/make-dummy-cert dummy_cert.txt


# docker run \
# --name docker-pgadmin4 \
# -e "PGADMIN_DEFAULT_EMAIL=admin" \
# -e "PGADMIN_DEFAULT_PASSWORD=docker" \
# -e "DEFAULT_USER=postgres" \
# --link docker-postgres:docker-postgres \
# -d dpage/pgadmin4 
#documentation
#https://docs.c9.io/docs/run-an-application
head -n 29 dummy_cert.txt>certificate.key
tail -n 26 dummy_cert.txt>certificate.cert
docker run  --name docker-pgadmin4 \
-v "/private/var/lib/pgadmin:/var/lib/pgadmin" \
-v "$(pwd)/certificate.cert:/certs/server.cert" \
-v "$(pwd)/certificate.key:/certs/server.key" \
-e "PGADMIN_DEFAULT_EMAIL=postgres" \
-e "PGADMIN_DEFAULT_PASSWORD=docker" \
-e "PGADMIN_ENABLE_TLS=True" \
--link docker-postgres:postgres \
-d dpage/pgadmin4 

docker run --name docker-nginx \
-p 8080:80 \
-v /home/ec2-user/environment/nginx.conf:/etc/nginx/nginx.conf:ro \
--link docker-pgadmin4:docker-pgadmin4 \
-d nginx
 

#echo https://$(curl http://169.254.169.254/latest/meta-data/instance-id).vfs.cloud9.us-east-2.amazonaws.com >index.html
#echo https://$(curl http://169.254.169.254/latest/meta-data/public-ipv4) >>index.html

#cat localhost.com
#sudo cp localhost.com /etc/nginx/sites-available/localhost.com
#sudo ln -s /etc/nginx/sites-available/localhost.com /etc/nginx/sites-enabled/localhost.com
#sudo service nginx restart
