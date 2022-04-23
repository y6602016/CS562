import psycopg2
import collections
from config import config
from datetime import date as dt
import string

class Formatter(string.Formatter):
    def __init__(self, missing = 'None', bad_fmt = '!!'):
        self.missing, self.bad_fmt = missing, bad_fmt

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super(Formatter, self).get_field(field_name, args, kwargs)
            # Python 3, 'super().get_field(field_name, args, kwargs)' works
        except (KeyError, AttributeError):
            val = None, field_name 
        return val 

    def format_field(self, value, spec):
        # handle an invalid format
        if value == None: return self.missing
        try:
            return super(Formatter, self).format_field(value, spec)
        except ValueError:
            if self.bad_fmt is not None: return self.bad_fmt   
            else: raise

def query():
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales'
  cursor.execute(query)
  rows = cursor.fetchall()
  
  mf_structure = {'cust': None, '1_sum_quant': None, '2_sum_quant': None, '3_sum_quant': None, '1_avg_quant': None, '3_avg_quant': None}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #1th Scan:
  for row in rows:
    #Grouping attributes:
    key_cust = row[0]
    if not group[(key_cust)]["cust"]:
      group[(key_cust)]["cust"] = key_cust


  #2th Scan:
  count_1_quant= collections.defaultdict(int)
  count_3_quant= collections.defaultdict(int)
  for (key_cust) in group:
    for row in rows:
      #Grouping attributes:
      cust = row[0]

      #Process Grouping Variable 1:
      quant = row[6]
      state = row[5]
      if group[(key_cust)]["cust"] == cust and state == "NY":
        if not group[(key_cust)]["1_sum_quant"]:
          group[(key_cust)]["1_sum_quant"] = quant
        else:
          group[(key_cust)]["1_sum_quant"] += quant

      if group[(key_cust)]["cust"] == cust and state == "NY":
        if not group[(key_cust)]["1_avg_quant"]:
          group[(key_cust)]["1_avg_quant"] = quant
          count_1_quant[(key_cust)] += 1
        else:
          count_1_quant[(key_cust)] += 1
          group[(key_cust)]["1_avg_quant"] += ((quant - group[(key_cust)]["1_avg_quant"])/count_1_quant[(key_cust)])

      #Process Grouping Variable 2:
      quant = row[6]
      state = row[5]
      if group[(key_cust)]["cust"] == cust and state == "NJ":
        if not group[(key_cust)]["2_sum_quant"]:
          group[(key_cust)]["2_sum_quant"] = quant
        else:
          group[(key_cust)]["2_sum_quant"] += quant


      #Process Grouping Variable 3:
      quant = row[6]
      state = row[5]
      if group[(key_cust)]["cust"] == cust and state == "CT":
        if not group[(key_cust)]["3_sum_quant"]:
          group[(key_cust)]["3_sum_quant"] = quant
        else:
          group[(key_cust)]["3_sum_quant"] += quant

      if group[(key_cust)]["cust"] == cust and state == "CT":
        if not group[(key_cust)]["3_avg_quant"]:
          group[(key_cust)]["3_avg_quant"] = quant
          count_3_quant[(key_cust)] += 1
        else:
          count_3_quant[(key_cust)] += 1
          group[(key_cust)]["3_avg_quant"] += ((quant - group[(key_cust)]["3_avg_quant"])/count_3_quant[(key_cust)])


  columns_type = []
  for val in group.values():
    columns_type.append(type(val["cust"]))
    columns_type.append(type(val["1_sum_quant"]))
    columns_type.append(type(val["2_sum_quant"]))
    columns_type.append(type(val["3_sum_quant"]))
    break

  row_formatter = []
  title_formatter = []
  date_index = []
  for i, t in enumerate(columns_type):
    if t == str or t == dt:
      row_formatter.append("{col" +str(i + 1) + ":<15}")
      title_formatter.append("{:<15}")
    elif t == float:
      row_formatter.append("{col" +str(i + 1) + ":>15,.2f}")
      title_formatter.append("{:<15}")
    else:
      row_formatter.append("{col" +str(i + 1) + ":>15}")
      title_formatter.append("{:<15}")
  title_formatter = "|".join(title_formatter)
  row_formatter = "|".join(row_formatter)
  print(title_formatter.format("cust", "1_sum_quant", "2_sum_quant", "3_sum_quant"))

  formater = Formatter()
  for val in group.values():
    data = {"col1": val["cust"], "col2": val["1_sum_quant"], "col3": val["2_sum_quant"], "col4": val["3_sum_quant"]}
    print(formater.format(row_formatter, **data))


if __name__ == "__main__":
  query()