import psycopg2
import collections
from config import config

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales'
  cursor.execute(query)
  
  mf_structure = {'cust': None, '1_sum_quant': None, '2_sum_quant': None, '3_sum_quant': None, '1_avg_quant': None, '3_avg_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #1th Scan:
  for row in cursor:
    cust = row[0]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust


  #2th Scan:
  cursor.execute(query)

  count_1_quant= collections.defaultdict(int)
  count_3_quant= collections.defaultdict(int)
  for row in cursor:
    cust = row[0]


    #Process Grouping Variable 1:
    state = row[5]
    quant = row[6]
    if group[(cust)]["cust"]==cust and state=="NY":
      if not group[(cust)]["1_sum_quant"]:
        group[(cust)]["1_sum_quant"] = quant
      else:
        group[(cust)]["1_sum_quant"] += quant

    if group[(cust)]["cust"]==cust and state=="NY":
      if not group[(cust)]["1_avg_quant"]:
        group[(cust)]["1_avg_quant"] = quant
        count_1_quant[(cust)] += 1
      else:
        count_1_quant[(cust)] += 1
        group[(cust)]["1_avg_quant"] += ((quant - group[(cust)]["1_avg_quant"])/count_1_quant[(cust)])


    #Process Grouping Variable 2:
    state = row[5]
    quant = row[6]
    if group[(cust)]["cust"]==cust and state=="NJ":
      if not group[(cust)]["2_sum_quant"]:
        group[(cust)]["2_sum_quant"] = quant
      else:
        group[(cust)]["2_sum_quant"] += quant



    #Process Grouping Variable 3:
    state = row[5]
    quant = row[6]
    if group[(cust)]["cust"]==cust and state=="CT":
      if not group[(cust)]["3_sum_quant"]:
        group[(cust)]["3_sum_quant"] = quant
      else:
        group[(cust)]["3_sum_quant"] += quant

    if group[(cust)]["cust"]==cust and state=="CT":
      if not group[(cust)]["3_avg_quant"]:
        group[(cust)]["3_avg_quant"] = quant
        count_3_quant[(cust)] += 1
      else:
        count_3_quant[(cust)] += 1
        group[(cust)]["3_avg_quant"] += ((quant - group[(cust)]["3_avg_quant"])/count_3_quant[(cust)])


  for val in group.values():
    print(val["cust"], val["1_sum_quant"], val["2_sum_quant"], val["3_sum_quant"], )

if __name__ == "__main__":
  query()