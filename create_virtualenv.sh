#!/bin/bash
if [ -d .py ]; then 
	echo "virtualenv already exists. skipping"
else 
	virtualenv .py -p python3
	.py/bin/python -m pip install -r requirements.txt
fi
