import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from multiprocessing.dummy import Pool

def data_to_s3(params):

	# throws error occured if there was a problem accessing data
	# otherwise downloads and uploads to s3

	rel = "G19"
	series = "47b3133fcba3957706678b2a55cb5a97"

	source_dataset_url = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel={}&series={}&lastobs=&from=&to=&filetype={}&label=include&layout=seriescolumn'.format(
		rel, series, params['filetype'])

	try:
		response = urlopen(source_dataset_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, params)

	except URLError as e:
		raise Exception('URLError: ', e.reason, params)

	else:
		data_set_name = os.environ['DATA_SET_NAME']
		filename = data_set_name + params['ext']
		file_location = '/tmp/' + data_set_name + params['ext']

		with open(file_location, 'wb') as f:
			f.write(response.read())

		# variables/resources used to upload to s3
		s3_bucket = os.environ['S3_BUCKET']
		new_s3_key = data_set_name + '/dataset/' + filename
		s3 = boto3.client('s3')

		s3.upload_file(file_location, s3_bucket, new_s3_key)

		print('Uploaded: ' + filename)

		# deletes to preserve limited space in aws lamdba
		os.remove(file_location)

		# dicts to be used to add assets to the dataset revision
		return {'Bucket': s3_bucket, 'Key': new_s3_key}

def source_dataset():

	# list of params to be used to access data included with product
	params = [
		{'filetype': 'csv', 'ext': '.csv'},
		{'filetype': 'spreadsheetml', 'ext': '.xls'},
		{'filetype': 'sdmx', 'ext': '.xml'}
	]

	# multithreading speed up accessing data, making lambda run quicker
	with (Pool(3)) as p:
		asset_list = p.map(data_to_s3, params)

	# asset_list is returned to be used in lamdba_handler function
	return asset_list