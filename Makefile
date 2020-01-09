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
	rm -rf exe/

exe: clean_exe
	pyinstaller src/py_dbmigration/data_load.py -w --onefile --distpath=exe
	tar -czvf artifact.tar -C exe/ .

buildbase:
	docker image rm buildbase:latest
	docker build -t buildbase -f Build.Dockerfile_base .

buildCentos6:
	./BuildTarget.sh
	mv ./artifacts/artifact.tar ./artifacts/py_dbmigration_centos6.tar

move_to_prod:
	cp ./artifacts/py_dbmigration_centos6.tar /runtime-exe/