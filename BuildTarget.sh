#git pull origin master

echo "version_dict = {\"git_hash\":\"\"\"">version.py
git rev-parse HEAD >>version.py
echo "\"\"\",">>version.py
echo "\"build_time\":\"\"\"">>version.py
date >>version.py
echo "\"\"\"">>version.py




echo "}">>version.py
#docker build -t buildbase -f BuildBase.Dockerfile .
docker build -t builder -f Build.Dockerfile .
docker rm buildmecentos
#docker run -it -v /tmp/deploy-ready/:/Build/output builder  
docker run -d --name buildmecentos builder 
#docker run -it --name buildmecentos -v /tmp/deploy-ready/:/Build/output builder cp switchboard_centos_6_10.tar /Build/output/
mkdir -p artifacts
docker cp buildmecentos:/Build/artifact.tar ./artifacts/artifact.tar 
#docker cp buildmecentos:/Build/dist ./artifacts/
# debug docker --rm -it <hash> sh
docker rm buildmecentos