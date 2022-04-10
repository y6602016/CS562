import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales where year = 2019'
  cursor.execute(query)
  
  mf_structure = {'cust': None, '1_sum_quant': None, '2_sum_quant': None, '3_sum_quant': None, '1_avg_quant': None, '3_avg_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))

  for row in cursor:
    cust = row[0]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust
  for val in group.values():
    print(val["cust"], val["1_sum_quant"], val["2_sum_quant"], val["3_sum_quant"], )

if __name__ == "__main__":
  query()