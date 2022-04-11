import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales'
  cursor.execute(query)
  
  mf_structure = {'cust': None, '0_avg_quant': None, '1_avg_quant': None, '2_avg_quant': None, '3_count_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #1th Scan:
  count_0_quant= collections.defaultdict(int)
  for row in cursor:
    cust = row[0]
    quant = row[6]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust
    if not group[(cust)]["0_avg_quant"]:
      group[(cust)]["0_avg_quant"] = quant
      count_0_quant[(cust)] += 1
    else:
      count_0_quant[(cust)] += 1
      group[(cust)]["0_avg_quant"] += ((quant - group[(cust)]["0_avg_quant"])/count_0_quant[(cust)])


  #2th Scan:
  cursor.execute(query)

  count_1_quant= collections.defaultdict(int)
  count_2_quant= collections.defaultdict(int)
  for row in cursor:
    cust = row[0]


    #Process Grouping Variable 1:
    quant = row[6]
    if group[(cust)]["cust"] == cust and quant > group[(cust)]["0_avg_quant"]:
      if not group[(cust)]["1_avg_quant"]:
        group[(cust)]["1_avg_quant"] = quant
        count_1_quant[(cust)] += 1
      else:
        count_1_quant[(cust)] += 1
        group[(cust)]["1_avg_quant"] += ((quant - group[(cust)]["1_avg_quant"])/count_1_quant[(cust)])


    #Process Grouping Variable 2:
    quant = row[6]
    if group[(cust)]["cust"] == cust and quant > group[(cust)]["0_avg_quant"]:
      if not group[(cust)]["2_avg_quant"]:
        group[(cust)]["2_avg_quant"] = quant
        count_2_quant[(cust)] += 1
      else:
        count_2_quant[(cust)] += 1
        group[(cust)]["2_avg_quant"] += ((quant - group[(cust)]["2_avg_quant"])/count_2_quant[(cust)])


  #3th Scan:
  cursor.execute(query)

  for row in cursor:
    cust = row[0]


    #Process Grouping Variable 3:
    quant = row[6]
    state = row[5]
    if group[(cust)]["cust"] == cust and state == 'NY' and quant > group[(cust)]["1_avg_quant"]:
      if not group[(cust)]["3_count_quant"]:
        group[(cust)]["3_count_quant"] = 1
      else:
        group[(cust)]["3_count_quant"] += 1



  for val in group.values():
    print(val["cust"], val["0_avg_quant"], val["1_avg_quant"], val["2_avg_quant"], val["3_count_quant"], )

if __name__ == "__main__":
  query()