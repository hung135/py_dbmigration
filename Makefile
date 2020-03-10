PHONY: build 
version_file=src/py_dbmigration/version.py
GIT_HASH := $(shell git rev-parse HEAD)
DATE:= ${shell date}
#the first remote will be used
GIT_HASH_URL:=$(shell git remote -v | head -n1 | sed -e"s/\t/ /g" | cut -d " " -f 2)
# all: clean build
# 	echo "Building ALL"
build: clean
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
	docker image rm buildbase:latest
	docker build -t buildbase -f Build.Dockerfile_base .

buildCentos6: version
	./BuildTarget.sh
	mv ./artifacts/artifact.tar ./artifacts/py_dbmigration_centos6.tar

move_to_prod: 
	cp ./artifacts/py_dbmigration_centos6.tar /runtime-exe/

rebuild_move: clean_exe buildCentos6
	#cp ./artifacts/py_dbmigration_centos6.tar /runtime-exe/
	tar -xvf ./artifacts/py_dbmigration_centos6.tar -C /runtime-exe/

test:
	/runtime-exe/data_load --yaml=/workspace/tests/data_load.yaml --ll=debug

sonar:
	sonar-scanner -Dsonar.projectKey=3af30f919bc788e20e7130d351c049f184e03844 \
	-Dsonar.sources=src \
	-Dsonar.host.url=http://sonarqube:9000 \
	-Dsonar.login=admin \
	-Dsonar.password=bitnami 
	echo http://localhost:9000/dashboard?id=3af30f919bc788e20e7130d351c049f184e03844
sonar_switchboard:
	sonar-scanner -Dsonar.projectKey=3af30f919bc788e20e7130d351c049f184e03845 \
	-Dsonar.sources=scripts \
	-Dsonar.projectBaseDir=/workspace/switchboard \
	-Dsonar.host.url=http://sonarqube:9000 \
	-Dsonar.login=admin \
	-Dsonar.password=bitnami 
	echo http://localhost:9000/dashboard?id=3af30f919bc788e20e7130d351c049f184e03845

python_test:
	python setup.py test


	