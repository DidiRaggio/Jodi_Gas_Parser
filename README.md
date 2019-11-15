# Jodi Gas Parser

## Os: Windows/Linux/Mac Os

## Python version: 3.6.1

## Dependencies:
	os
	zipfile
	pandas
	tqdm
	requests
	csv
	json

## Usage:
	0) Open terminal and cd to working directory.
	Option 1: Run Script
		1) run terminal command:
			$ python parser.py
	Option 2: Jupyter Notebook
		1) run terminal command:
			$ juptyer notebook
		2) execute each cell with "crl + enter"

## Notes:
	1) The 'TOTDEMC', 'TOTIMPSB', 'TOTDEMO', 'MAINTOT', 'TOTEXPSB', 'CLOSTLV', 'CONVER', 'OSOURCES' codes for the FLOW_BREAKDOWNS where not found in the manual.
	2) You can set the output TIME_PERIOD to pandas timestamp by setting time_period_timestamp to True on line # 50.
	3) The zipfile can be downloaded by setting the 'download_file=True' as an argument in the task() function. By default it streams the zipfile in temporary memory.