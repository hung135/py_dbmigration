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
	python setup.py release
	#twine upload dist/*
clean_build: clean build
clean:
	rm -rf build/ dist/