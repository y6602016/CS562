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
  
  mf_structure = {'cust': None, '1.quant': None, '1.state': None, '1.date': None, '1_max_date': None, '0_sum_quant': None, '1_min_quant': None, '1_sum_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #1th Scan:
  for row in cursor:
    #Grouping attributes:
    cust = row[0]
    quant = row[6]
    if not group[(cust)]["cust"]:
      group[(cust)]["cust"] = cust
    if not group[(cust)]["0_sum_quant"]:
      group[(cust)]["0_sum_quant"] = quant
    else:
      group[(cust)]["0_sum_quant"] += quant



  #2th Scan:
  cursor.execute(query)

  for row in cursor:
    #Grouping attributes:
    cust = row[0]

    #Process Grouping Variable 1:
    quant = row[6]
    state = row[5]
    date = row[7]
    if group[(cust)]["cust"] == cust and date > dt.fromisoformat("2019-05-31") and date < dt.fromisoformat("2019-09-01"):
      if not group[(cust)]["1_min_quant"]:
        group[(cust)]["1_min_quant"] = quant
        group[(cust)]["1.date"] = date
        group[(cust)]["1.quant"] = quant
        group[(cust)]["1.state"] = state
      else:
        if quant < group[(cust)]["1_min_quant"]:
          group[(cust)]["1_min_quant"] = quant
          group[(cust)]["1.date"] = date
          group[(cust)]["1.quant"] = quant
          group[(cust)]["1.state"] = state
    if group[(cust)]["cust"] == cust and date > dt.fromisoformat("2019-05-31") and date < dt.fromisoformat("2019-09-01"):
      if not group[(cust)]["1_sum_quant"]:
        group[(cust)]["1_sum_quant"] = quant
      else:
        group[(cust)]["1_sum_quant"] += quant

    if group[(cust)]["cust"] == cust and date > dt.fromisoformat("2019-05-31") and date < dt.fromisoformat("2019-09-01"):
      if not group[(cust)]["1_max_date"]:
        group[(cust)]["1_max_date"] = date
      else:
        if date > group[(cust)]["1_max_date"]:
          group[(cust)]["1_max_date"] = date


  columns_type = []
  for val in group.values():
    columns_type.append(type(val["cust"]))
    columns_type.append(type(val["1.quant"]))
    columns_type.append(type(val["1.state"]))
    columns_type.append(type(val["1.date"]))
    columns_type.append(type(val["1_max_date"]))
    break

  row_formatter = []
  title_formatter = []
  date_index = []
  for t in columns_type:
    if t == str or t == dt:
      row_formatter.append("{:<15}")
      title_formatter.append("{:<15}")
    elif t == float:
      row_formatter.append("{:>15,.2f}")
      title_formatter.append("{:>15}")
    else:
      row_formatter.append("{:>15}")
      title_formatter.append("{:>15}")
  title_formatter = " ".join(title_formatter)
  row_formatter = " ".join(row_formatter)
  print(title_formatter.format("cust", "1.quant", "1.state", "1.date", "1_max_date"))

  for val in group.values():
    if val["1_sum_quant"] * 30 > val["0_sum_quant"] and val["1.quant"] == val["1_min_quant"] and val["1.date"] > dt.fromisoformat("2019-06-06"):
      print(row_formatter.format(val["cust"], val["1.quant"], val["1.state"], str(val["1.date"]), str(val["1_max_date"])))

if __name__ == "__main__":
  query()