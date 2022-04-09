import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales where year = 2019'
  cursor.execute(query)
  
  mf_structure = {'cust': None, '0_sum_quant': None, '0_avg_quant': None, '0_max_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))

  count_quant= collections.defaultdict(int)
  for row in cursor:
    cust = row[0]
    quant = row[6]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust
    if not group[(cust)]["0_sum_quant"]:
      group[(cust)]["0_sum_quant"] = quant
    else:
      group[(cust)]["0_sum_quant"] += quant

    if not group[(cust)]["0_avg_quant"]:
      group[(cust)]["0_avg_quant"] = quant
      count_quant[(cust)] += 1
    else:
      count_quant[(cust)] += 1
      group[(cust)]["0_avg_quant"] += ((quant - group[(cust)]["0_avg_quant"])/count_quant[(cust)])
    if not group[(cust)]["0_max_quant"]:
      group[(cust)]["0_max_quant"] = quant
    else:
      group[(cust)]["0_max_quant"] = max(quant, group[(cust)]["0_max_quant"])

  for val in group.values():
    print(val["cust"], val["0_sum_quant"], val["0_avg_quant"], val["0_max_quant"], )

if __name__ == "__main__":
  query()