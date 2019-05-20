#!/usr/bin/env bash
for i in /home/dtdata/source_data/compliance_toolkit/processed/*.csv
do
	head -1 "${i}" >> headedfile.txt
	echo "$i" >> headedfile.txt
done

