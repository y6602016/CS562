import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cur = conn.cursor()
  mf_structure = {
				'cust': None,
				'1_sum_quant': None,
				'2_sum_quant': None,
				'3_sum_quant': None,
				'1_avg_quant': None,
				'3_avg_quant': None
		}
