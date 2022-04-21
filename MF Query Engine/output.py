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
  
  mf_structure = {'cust': None, '1.quant': None, '1.state': None, '1.date': None, '0_sum_quant': None, '1_min_quant': None, '1_sum_quant': None}
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
    date = row[7]
    state = row[5]
    quant = row[6]
    if group[(cust)]["cust"] == cust and date > dt.fromisoformat("2019-05-31") and date < dt.fromisoformat("2019-09-01"):
      if not group[(cust)]["1_min_quant"]:
        group[(cust)]["1_min_quant"] = quant
        group[(cust)]["1.date"] = date
        group[(cust)]["1.state"] = state
        group[(cust)]["1.quant"] = quant
      else:
        if quant < group[(cust)]["1_min_quant"]:
          group[(cust)]["1_min_quant"] = quant
          group[(cust)]["1.date"] = date
          group[(cust)]["1.state"] = state
          group[(cust)]["1.quant"] = quant
    if group[(cust)]["cust"] == cust and date > dt.fromisoformat("2019-05-31") and date < dt.fromisoformat("2019-09-01"):
      if not group[(cust)]["1_sum_quant"]:
        group[(cust)]["1_sum_quant"] = quant
      else:
        group[(cust)]["1_sum_quant"] += quant



  columns_type = []
  for val in group.values():
    columns_type.append(type(val["cust"]))
    columns_type.append(type(val["1.quant"]))
    columns_type.append(type(val["1.state"]))
    columns_type.append(type(val["1.date"]))
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
  print(title_formatter.format("cust", "1.quant", "1.state", "1.date"))

  formater = Formatter()
  for val in group.values():
    if val["1_sum_quant"] * 30 > val["0_sum_quant"] and val["1.quant"] == val["1_min_quant"] and val["1.date"] > dt.fromisoformat("2019-06-06"):
      data = {"col1": val["cust"], "col2": val["1.quant"], "col3": val["1.state"], "col4": str(val["1.date"])}
      print(formater.format(row_formatter, **data))


if __name__ == "__main__":
  query()