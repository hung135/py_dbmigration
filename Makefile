PHONY: build 
version_file=src/py_dbmigration/version.py
GIT_HASH := $(shell git rev-parse HEAD)
DATE:= ${shell date}
DANGLING:= $(shell docker images -f "dangling=true" -q)
STOPPED:=$(shell docker container ls -aq)
#the first remote will be used
GIT_HASH_URL:=$(shell git remote -v | head -n1 | sed -e"s/\t/ /g" | cut -d " " -f 2)
# all: clean build
# 	echo "Building ALL"
build: clean version bumpversion
	#python setup.py sdist bdist_wheel build
	python setup.py build
dist:
	#python setup.py sdist bdist_wheel build
	python setup.py dist

release:
	#python setup.py release
	twine upload dist/* --verbose
cleanbuild: clean build
clean:
	rm -rf build/ dist/ exe/
clean_exe:
	rm -rf exe/ artifacts/

version:
	echo "version = {">${version_file}>${version_file}
	
	echo "\"git_hash\":\"${GIT_HASH}\",">>${version_file}
	echo "\"url\":\"${GIT_HASH_URL}/commit/${GIT_HASH}\",">>${version_file}
	echo "\"check_out_syntax\":\"git checkout ${GIT_HASH} .\"," >>${version_file}
	echo "\"build_time\":\"${DATE}\"">>${version_file}
	 
	echo "}">>${version_file}

	cat ${version_file}
exe: clean_exe 
	# pyinstaller src/py_dbmigration/data_load.py -w --onefile \
	# --distpath=exe \
	# --add-data 'src/py_dbmigration/data_file_mgnt/logic_sql.yml:py_dbmigration/data_file_mgnt/' \
	# --hidden-import=py_dbmigration.custom_logic.load_status \
	# --hidden-import=py_dbmigration.custom_logic.generate_checksum 

	# need to make sure all dependency exists so pyinstall can crawl and package them also
	pip install -r src/py_dbmigration/requirements.txt 

	pyinstaller ./data_load.spec --distpath=exe
	tar -czvf artifact.tar -C exe/ .

buildbase:
	docker image rm buildbase:latest || true
	docker build -t buildbase -f Build.Dockerfile_base .

buildCentos6: version
	./BuildTarget.sh
	mv ./artifacts/artifact.tar ./artifacts/py_dbmigration_centos6.tar
buildCentos7: version
	./BuildTarget.sh
	mv ./artifacts/artifact.tar ./artifacts/py_dbmigration_centos7.tar

move_to_prod: 
	cp ./artifacts/py_dbmigration_centos6.tar /runtime-exe/
test_to_prod: buildCentos6
	tar -xvf ./artifacts/py_dbmigration_centos6.tar -C /runtime-exe/tests/

rebuild_move: clean_exe buildCentos6
	#cp ./artifacts/py_dbmigration_centos6.tar /runtime-exe/
	tar -xvf ./artifacts/py_dbmigration_centos6.tar -C /runtime-exe/

test:
	/runtime-exe/data_load --yaml=/workspace/tests/data_load.yaml --ll=debug

 

python_test:
	clear
	pytest /workspace/tests/


testplugin: clean_meta
	clear
	python /workspace/src/py_dbmigration/data_load.py  --yaml=/workspace/tests/data_load_plugin.yaml --ll=debug

testpluginexe: clean_meta
	clear
	/workspace/exe/data_load  --yaml=/workspace/tests/data_load_plugin.yaml --ll=20

clean_meta:
	psql -c"truncate table logging.meta_source_files;"

pytest:
	pytest tests/test_pidmanager.py 

bumpversion:
	bumpversion patch 


#docker inspect $(docker ps | awk '{ print $NF }' | grep pgdb) | grep NetworkID | awk -F '"' '{print $4}'
#containerid=$(shell docker ps | awk '{ print $$NF }' | grep pgdb)
networkid:= $(shell docker inspect $$(docker ps | awk '{ print $$NF }' | grep pgdb) | grep NetworkID | awk -F '"' '{print $$4}')
network:=--network="${networkid}"
envfile:=--env-file="./tests/env.txt"
# this is used to spin up a base centos container and run the built executable to check if dependencies were properly packaged
testexe: 
	#data_load  --yaml=data_load_plugin.yaml 
	bash BuildTargetTest.sh
	psql -c"create schema logging" || true
	docker run --rm ${envfile} ${network} buildtest:latest env
	docker run --rm ${envfile} ${network} buildtest:latest /exe/data_load --yaml=run_once.yaml
remove_dangling:
	 
	docker container stop ${STOPPED}
	docker rmi ${DANGLING}