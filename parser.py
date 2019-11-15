import os
import zipfile
import pandas as pd
from tqdm import tqdm
import requests
import csv
import json
try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

###################################
# INITIALIZE REFERENCES
###################################
with open('country_abbreviations.json') as json_file:
    country_abbreviations = json.load(json_file)

energy_product_dict = {
    'NATGAS': 'Natural Gas',
    'LNG': 'Liquid Natural Gas'
}

flow_breakdown_dict = {
    'EXPLNG': 'Export Liquid Natural Gas', 
    'IMPLNG': 'Import Liquid Natural Gas', 
    'INDPROD': 'Industrial Production', 
    'TOTDEMC': 'TOTDEMC',
    'TOTIMPSB': 'TOTIMPSB',
    'TOTDEMO': 'TOTDEMO',
    'MAINTOT': 'MAINTOT',
    'TOTEXPSB': 'TOTEXPSB',
    'IMPPIP': 'Import Pipeline', 
    'STOCKCH': 'Stock Change',
    'EXPPIP': 'Export Pipeline', 
    'STATDIFF': 'Statistical Difference',
    'CLOSTLV': 'CLOSTLV',
    'CONVER': 'CONVER',
    'OSOURCES': 'OSOURCES',
}

unit_measure_dict = {
    'M3': 'Cubic Meters', 
    'TJ': 'Terajoule', 
    'KTONS': 'Kilotons', 
    'M3_T': 'Metric Tons', 
    'KT': 'Kiloton'
}

time_period_timestamp = False

def stream_zip():
	output = BytesIO()
	chunk_size = 1024
	 
	url = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"
	 
	req = requests.get(url, stream = True)
	 
	total_size = int(req.headers['content-length'])

	for data in tqdm(iterable=req.iter_content(chunk_size=chunk_size), total = total_size/chunk_size, unit='KB'):
	    output.write(data)
	print("Download Completed!!!")

	return output
            
def download_zip():
	
	chunk_size = 1024
	 
	url = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"
	 
	req = requests.get(url, stream = True)
	 
	total_size = int(req.headers['content-length'])
	
	try: 
		with open("jodi_gas_csv_beta.zip", "wb") as file:
		    for data in tqdm(iterable=req.iter_content(chunk_size=chunk_size), total = total_size/chunk_size, unit='KB'):
		        file.write(data)
		print("Download Completed!!!")
	except Exception as e:
		print("There was an error: {0}".format(e))

def unzip_and_read_csv(zip_file):
	with zipfile.ZipFile(zip_file) as z:
	    for filename in z.namelist():
	        if not os.path.isdir(filename):
	            print("in dir")
	            file = z.open(filename)
	            df = pd.read_csv(file, parse_dates=["TIME_PERIOD"], index_col="TIME_PERIOD")
	            return df

def parse_df(df):
	json_array = []
	groups = df.groupby(['REF_AREA','ENERGY_PRODUCT', 'FLOW_BREAKDOWN', 'UNIT_MEASURE', 'ASSESSMENT_CODE'])
	for group, group_df in groups:
		points = []
		for row_index, row in group_df.iterrows():
			if time_period_timestamp:
				time_period = row_index
			else:
				time_period = row_index.strftime('%Y-%m-%d')
			points.append([time_period, row['OBS_VALUE']])
		json_group = {
			"series_id": "joi-gas-data\\{0}\\{1}\\{2}\\{3}\\{4}".format(group[0],group[1],group[2],group[3],group[4]),
			"points": points,
			"fields": {
				"country": next((item["country"] for item in country_abbreviations if item["abbreviation"] == group[0]), None),
				"measurement unit": unit_measure_dict[group[3]],
				"assessment code": group[4],
				"flow breakdown": flow_breakdown_dict[group[2]],
				"energy product": energy_product_dict[group[1]]
			}
		}
		json_array.append(json_group)
		print(str(json_group) + "\n")
	return json_array

def task(download_file=False):
	if download_file:
		download_zip()
		file = 'jodi_gas_csv_beta.zip'
	else:
		file = stream_zip()
	df = unzip_and_read_csv(file)
	return parse_df(df)

if __name__ == '__main__':
	task()