import psycopg2
import csv
import urllib.request
import py7zr
import os
import configparser
import sys
import time


YEARS = ['2016', '2017', '2018', '2019', '2020']
COLUMNS = ['outid', 'birth', 'sextypename', 'regname', 'areaname', 'tername', 'regtypename', 'eoname', 'eotypename', 'eoregname', 'eoareaname', 'eotername', 'eoparent', 'ukrtest', 'ukrteststatus', 'ukrball100', 'ukrball12', 'ukrptname', 'ukrptregname', 'ukrptareaname', 'ukrpttername', 'histtest', 'histlang', 'histteststatus', 'histball100', 'histball12', 'histptname', 'histptregname', 'histptareaname', 'histpttername', 'mathtest', 'mathlang', 'mathteststatus', 'mathball100', 'mathball12', 'mathptname', 'mathptregname', 'mathptareaname', 'mathpttername', 'phystest', 'physlang', 'physteststatus', 'physball100', 'physptname', 'physptregname', 'physptareaname', 'physpttername', 'chemtest', 'chemlang', 'chemteststatus', 'chemball100', 'chemptname', 'chemptregname', 'chemptareaname', 'chempttername', 'biotest', 'biolang', 'bioteststatus', 'bioball100', 'bioptname', 'bioptregname', 'bioptareaname', 'biopttername', 'geotest', 'geolang', 'geoteststatus', 'geoball100', 'geoptname', 'geoptregname', 'geoptareaname', 'geopttername', 'engtest', 'engteststatus', 'engball100', 'engptname', 'engptregname', 'engptareaname', 'engpttername', 'frtest', 'frteststatus', 'frball100', 'frptname', 'frptregname', 'frptareaname', 'frpttername', 'deutest', 'deuteststatus', 'deuball100', 'deuptname', 'deuptregname', 'deuptareaname', 'deupttername', 'sptest', 'spteststatus', 'spball100', 'spptname', 'spptregname', 'spptareaname', 'sppttername', 'rustest', 'rusteststatus', 'rusball100', 'rusptname', 'rusptregname', 'rusptareaname', 'ruspttername', 'stid', 'tertypename', 'classprofilename', 'classlangname', 'physball12', 'chemball12', 'bioball12', 'geoball12', 'engball12', 'fratest', 'frateststatus', 'fraball100', 'fraball12', 'fraptname', 'fraptregname', 'fraptareaname', 'frapttername', 'deuball12', 'spatest', 'spateststatus', 'spaball100', 'spaball12', 'spaptname', 'spaptregname', 'spaptareaname', 'spapttername', 'rusball12', 'ukrball', 'histball', 'mathball', 'physball', 'chemball', 'bioball', 'geoball', 'engdpalevel', 'engball', 'fradpalevel', 'fraball', 'deudpalevel', 'deuball', 'spadpalevel', 'spaball', 'ukradaptscale']
TABLENAME = 'eng_results'
RESULTS_FILENAME = 'Results.csv'
RESULT_HEADER = ['Region', 'Year', 'Mark']
MAX_TRY_COUNT = 3


def download(year):
	url = f'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO{year}.7z'
	try:
		with urllib.request.urlopen(url) as request:
			filesize = 0
			filename = f'data_{year}.7z'
			with open(filename, 'wb') as file:
				filesize += file.write(request.read())
		return filename
	except:
		print(f'Couldn\'t download file from {url}')


def extract(filename, year):
	with py7zr.SevenZipFile(filename, 'r') as archive:
		archive.extract(targets=[f'Odata{year}File.csv', f'OpenData{year}.csv'])
	if os.path.exists(f'Odata{year}File.csv'):
		os.rename(f'Odata{year}File.csv', f'OpenData{year}.csv')


def try_get_data(year):
	print(f'Downloading data file for {year}')
	filename = download(year)
	print(f'Downloaded file {filename}. Extracting...')
	extract(filename, year)
	print(f'Extracted\n')


def try_connect(conn_config):
	conn = psycopg2.connect(dbname=conn_config['dbname'],
							user=conn_config['user'],
							password=conn_config['password'],
							host=conn_config['host'])
	if conn != None:
		print(f'Connected to {conn_config["dbname"]}\n')
	return conn


def try_insert_data(conn, year):

	def get_encoding(year):
		if year in ['2017', '2018']:
			return 'utf-8-sig'
		else:
			return None

	with conn:
		with conn.cursor() as cur:

			filename = f'OpenData{year}.csv'
			with open(filename, 'r', newline='', encoding=get_encoding(year)) as file:
				print(f'Processing file {filename}')
				data = csv.reader(file, delimiter=';', quotechar='"')
				header = list(map(str.lower, next(data)))
				num_of_rows = 0

				for row in data:
					num_of_rows += 1
					insert_row = [None]*len(COLUMNS)
					for i, col in enumerate(header):
						index = COLUMNS.index(col)
						insert_row[index] = try_convert_type(row[i])
					insert_row = (*insert_row, year)

					try_insert(insert_row, conn)

				print(f'Inserted {num_of_rows} rows from {filename} into table {TABLENAME}')



def try_select(conn):
	with conn:
		with conn.cursor() as cur:

			execute_line = '''SELECT regname AS region, "year", MIN(engball100) AS ball
							  FROM eng_results
							  WHERE engteststatus IN ('Зараховано', 'Отримав результат') AND "year" IN (2019, 2020)
							  GROUP BY region, "year"
							  ORDER BY region, "year";'''
			cur.execute(execute_line)
			data = cur.fetchall()

			with open(RESULTS_FILENAME, 'w', newline='') as file:
				writer = csv.writer(file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
				writer.writerow(RESULT_HEADER)
				for row in data:
					writer.writerow(row)
			print(f"\nCreated file {RESULTS_FILENAME}")


def retry(func, msg, *args, **kwargs):
	try_count = 0
	while try_count < MAX_TRY_COUNT:
		try:
			return func(*args, **kwargs)
		except Exception as exc:
			try_count += 1
			print(exc)
	else:
		print(msg)
		sys.exit()


def try_convert_type(value):
	if value=='null':
		return None
	try:
		res = float(value.replace(',', '.'))
		return res
	except:
		return value


def try_insert(insert_row, conn):
	try:
		with conn.cursor() as cur:
			execute_line = f'''INSERT INTO {TABLENAME} ({','.join(COLUMNS)}, year)
							   VALUES ({'%s, '*(len(COLUMNS))}%s)'''
			cur.execute(execute_line, insert_row)
	except psycopg2.errors.UniqueViolation:
		pass
	except psycopg2.errors.InFailedSqlTransaction:
		pass
	except Exception as e:
		print(e)
		sys.exit()


if __name__ == '__main__':

	config = configparser.ConfigParser()
	config.read('settings.ini')
	conn_config = config['connection']

	# for year in YEARS:
	# 	retry(try_get_data, 'Couldn\'t download data', year)

	conn = retry(try_connect, 'Couldn\'t connect to DB', conn_config)

	start = time.time()
	for year in YEARS:
		retry(try_insert_data, 'Failed inserting data', conn, year)
	duration = time.time()-start
	with open('Duration.txt', 'w') as file:
		file.write(f'Duration of inserting data from all the years is {duration}')

	retry(try_select, 'Failed selecting data', conn)			

	conn.close()
