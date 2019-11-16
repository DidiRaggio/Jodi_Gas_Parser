import os
import zipfile
import requests
import json

try:
	import pandas as pd
	from tqdm import tqdm
	standard_libraries = False
except ImportError:
	import csv
	standard_libraries = True

try:
    from BytesIO import BytesIO ## for Python 2
except ImportError:
    from io import BytesIO ## for Python 3

###################################
# INITIALIZE REFERENCES
###################################
with open("country_abbreviations.json") as json_file:
    country_abbreviations = json.load(json_file)

energy_product_dict = {
    "NATGAS": "Natural Gas",
    "LNG": "Liquid Natural Gas"
}

flow_breakdown_dict = {
    "EXPLNG": "Export Liquid Natural Gas", 
    "IMPLNG": "Import Liquid Natural Gas", 
    "INDPROD": "Industrial Production", 
    "TOTDEMC": "TOTDEMC",
    "TOTIMPSB": "TOTIMPSB",
    "TOTDEMO": "TOTDEMO",
    "MAINTOT": "MAINTOT",
    "TOTEXPSB": "TOTEXPSB",
    "IMPPIP": "Import Pipeline", 
    "STOCKCH": "Stock Change",
    "EXPPIP": "Export Pipeline", 
    "STATDIFF": "Statistical Difference",
    "CLOSTLV": "CLOSTLV",
    "CONVER": "CONVER",
    "OSOURCES": "OSOURCES",
}

unit_measure_dict = {
    "M3": "Cubic Meters", 
    "TJ": "Terajoule", 
    "KTONS": "Kilotons", 
    "M3_T": "Metric Tons", 
    "KT": "Kiloton"
}

time_period_timestamp = False
output_file = False

def stream_zip():
	
	output = BytesIO()
	url = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"

	if standard_libraries:
		# download the file contents in binary format
		req = requests.get(url)
		 
		output.write(req.content)
	 
	else:
	
		chunk_size = 1024
		req = requests.get(url, stream = True)
		 
		total_size = int(req.headers["content-length"])

		for data in tqdm(iterable=req.iter_content(chunk_size=chunk_size), total = total_size/chunk_size, unit="KB"):
		    output.write(data)
	
	print("Download Completed!!!")

	return output

            
def download_zip():
	
	url = "https://www.jodidata.org/_resources/files/downloads/gas-data/jodi_gas_csv_beta.zip"

	try: 
		with open("jodi_gas_csv_beta.zip", "wb") as file:
			if standard_libraries:
				# download the file contents in binary format
				req = requests.get(url)
				print("Download Completed!!!")
				file.write(req.content)
			else:
				chunk_size = 1024
		 
				req = requests.get(url, stream = True)
				 
				total_size = int(req.headers["content-length"])
				for data in tqdm(iterable=req.iter_content(chunk_size=chunk_size), total = total_size/chunk_size, unit="KB"):
					file.write(data)
				print("Download Completed!!!")
	except Exception as e:
		print("There was an error: {0}".format(e))



def unzip_and_read_csv(zip_file):
	with zipfile.ZipFile(zip_file) as z:
		for filename in z.namelist():
			if not os.path.isdir(filename):
				if filename.endswith(".csv"):
					file = z.open(filename)
					if standard_libraries:
						file_str_iterator = file.read().decode("utf-8").splitlines()
						csv_reader = csv.reader(file_str_iterator, delimiter=",")
						return csv_reader
					else:
						df = pd.read_csv(file, parse_dates=["TIME_PERIOD"], index_col="TIME_PERIOD")
						return df

def parse_df(df):
	json_array = []
	groups = df.groupby(["REF_AREA","ENERGY_PRODUCT", "FLOW_BREAKDOWN", "UNIT_MEASURE", "ASSESSMENT_CODE"])
	for group, group_df in groups:
		points = []
		for row_index, row in group_df.iterrows():
			if time_period_timestamp:
				time_period = row_index
			else:
				time_period = row_index.strftime("%Y-%m")
			points.append([time_period, row["OBS_VALUE"]])
		json_series = {
			"series_id": "joi-gas-data\\{0}\\{1}\\{2}\\{3}\\{4}".format(group[0],group[1],group[2],group[3],group[4]),
			"points": points,
			"fields": {
				"country": next((item["country"] for item in country_abbreviations if item["abbreviation"] == group[0]), None),
				"measurement unit": unit_measure_dict[group[3]],
				"assessment code": int(group[4]),
				"flow breakdown": flow_breakdown_dict[group[2]],
				"energy product": energy_product_dict[group[1]]
			}
		}
		json_array.append(json_series)

	if output_file:
		with open("data.json", "w") as outfile:
			json.dump(json_array, outfile)
		
	return json_array

def parse_csv(csv_reader):
	json_array = []
	for idx, row in enumerate(csv_reader):
		if  idx > 0:
			series_id = "joi-gas-data\\{0}\\{1}\\{2}\\{3}\\{4}".format(row[0],row[2],row[3],row[4],row[6])
			json_series = [item for item in json_array if item["series_id"] == series_id]
			if json_series:
				point = [row[1], float(row[5])]
				json_series[0]["points"].append(point)
			else:
				json_series = {
					"series_id": series_id,
					"points": [[row[1],float(row[5])]],
					"fields": {
						"country": next((item["country"] for item in country_abbreviations if item["abbreviation"] == row[0]), None),
						"measurement unit": unit_measure_dict[row[4]],
						"assessment code": int(row[6]),
						"flow breakdown": flow_breakdown_dict[row[3]],
						"energy product": energy_product_dict[row[2]]
					}
				}
				
				json_array.append(json_series)
				
	if output_file:
		with open("data.json", "w") as outfile:
			json.dump(json_array, outfile)
		return json_array

def print_json(json_array):
	for json_series in json_array:
		print(json_series)


def task(download_file=False):
	if download_file:
		download_zip()
		file = "jodi_gas_csv_beta.zip"
	else:
		file = stream_zip()
	data = unzip_and_read_csv(file)
	json_array = (parse_csv(data) if standard_libraries else parse_df(data))
	print_json(json_array)
	return json_array

if __name__ == "__main__":

	###########################
	# OPTIONS:
	###########################
	# output_file = True
	# standard_libraries = True
	# time_period_timestamp = True

	task()
