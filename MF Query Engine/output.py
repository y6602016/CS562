import psycopg2
import collections
from config import config
from datetime import date as dt

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales'
  cursor.execute(query)
  
  mf_structure = {'cust': None, '1.quant': None, '1.state': None, '1.date': None, '0_max_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #1th Scan:
  for row in cursor:
    #Grouping attributes:
    cust = row[0]
    quant = row[6]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust
    if not group[(cust)]["0_max_quant"]:
      group[(cust)]["0_max_quant"] = quant
    else:
      if quant > group[(cust)]["0_max_quant"]:
        group[(cust)]["0_max_quant"] = quant


  #2th Scan:
  cursor.execute(query)

  for row in cursor:
    #Grouping attributes:
    cust = row[0]

    #Process Grouping Variable 1:
    state = row[5]
    date = row[7]
    quant = row[6]
    if group[(cust)]["cust"] == cust and quant == group[(cust)]["0_max_quant"]:
      group[(cust)]["1.quant"] = quant
      group[(cust)]["1.state"] = state
      group[(cust)]["1.date"] = date


  for val in group.values():
    print(val["cust"], val["1.quant"], val["1.state"], val["1.date"], )

if __name__ == "__main__":
  query()