import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('settings.ini')
conn_config = config['connection']

conn = psycopg2.connect(dbname=conn_config['dbname'],
						user=conn_config['user'],
						password=conn_config['password'],
						host=conn_config['host'])

with conn:
	with conn.cursor() as cur:
		create_table_query = '''CREATE TABLE public.eng_results
								(
								    outid character varying(127) COLLATE pg_catalog."default" NOT NULL,
								    regname character varying(127) COLLATE pg_catalog."default",
								    engteststatus character varying(127) COLLATE pg_catalog."default",
								    engball100 integer,
								    year integer,
								    CONSTRAINT eng_results_pkey PRIMARY KEY (outid)
								)'''
		cur.execute(create_table_query)

conn.close()