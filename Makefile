PHONY: build 

# all: clean build
# 	echo "Building ALL"
build:
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


exe: clean_exe
	# pyinstaller src/py_dbmigration/data_load.py -w --onefile \
	# --distpath=exe \
	# --add-data 'src/py_dbmigration/data_file_mgnt/logic_sql.yml:py_dbmigration/data_file_mgnt/' \
	# --hidden-import=py_dbmigration.custom_logic.load_status \
	# --hidden-import=py_dbmigration.custom_logic.generate_checksum 	
	pyinstaller ./data_load.spec --distpath=exe
	tar -czvf artifact.tar -C exe/ .

buildbase:
	docker image rm buildbase:latest
	docker build -t buildbase -f Build.Dockerfile_base .

buildCentos6:
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