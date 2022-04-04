import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales where year = 2019'
  cursor.execute(query)
  
  mf_structure = {'cust': None, 'prod': None, '0_avg_quant': None, '0_max_quant': None}
  group = collections.defaultdict(lambda:mf_structure)

  count = collections.defaultdict(int)
  for row in cursor:
    cust = row[0]
    prod = row[1]
    quant = row[6]
    if not group[(cust, prod)]["0_avg_quant"]:
      group[(cust, prod)]["0_avg_quant"] = quant
    else:
      group[(cust, prod)]["0_avg_quant"] += quant
    count[(cust, prod)] += 1
    if not group[(cust, prod)]["0_max_quant"]:
      group[(cust, prod)]["0_max_quant"] = quant
    else:
      group[(cust, prod)]["0_max_quant"] = max(quant, group[(cust, prod)]["0_max_quant"])

  for key, val in group.items():
    print(key[0], key[1], val['0_avg_quant'] / count[key], val['0_max_quant'])

if __name__ == "__main__":
  query()