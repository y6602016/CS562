import psycopg2
import collections
from Config.config import config
from datetime import date as dt
import string

#==============================================
#= The class used to handle output None value =
#==============================================
class Formatter(string.Formatter):
    def __init__(self, missing = '%-11s%-0s'%("", "None"), bad_fmt = '!!'):
        self.missing, self.bad_fmt = missing, bad_fmt

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super(Formatter, self).get_field(field_name, args, kwargs)
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
  #===================================================
  #= connnect to db and use the cursor to query data =
  #===================================================
  params = config()
  conn = psycopg2.connect(**params)
  cursor = conn.cursor()

  query = 'select * from sales'
  cursor.execute(query)
  rows = cursor.fetchall()
  


  #====================================================================================
  #= the data structure of mf_structure is hashtable                                  =
  #= group is a hashtable with grouping attributes as keys and mf_structure as values =
  #====================================================================================
  mf_structure = {'cust': None, 'prod': None, '0_avg_quant': None, '1_avg_quant': None, '1.quant': None, '1.state': None, '1.date': None, '1_max_quant': None}
  mf_type = {'cust': 'str', 'prod': 'str', '0_avg_quant': 'float', '1_avg_quant': 'float', '1.quant': 'int', '1.state': 'str', '1.date': 'dt', '1_max_quant': 'int'}
  group = collections.defaultdict(lambda: dict(mf_structure))



  #=====================================================
  #= the first scan to fill all grouping attributes    =
  #= and aggregatation function of grouping variable_0 =
  #=====================================================

  #1th Scan:
  count_0_quant= collections.defaultdict(int)
  for row in rows:
    #Grouping attributes:
    key_cust = row[0]
    key_prod = row[1]
    quant = row[6]
    if not group[(key_cust, key_prod)]["cust"]:
      group[(key_cust, key_prod)]["cust"] = key_cust
      group[(key_cust, key_prod)]["prod"] = key_prod
    if not group[(key_cust, key_prod)]["0_avg_quant"]:
      group[(key_cust, key_prod)]["0_avg_quant"] = quant
      count_0_quant[(key_cust, key_prod)] += 1
    else:
      count_0_quant[(key_cust, key_prod)] += 1
      group[(key_cust, key_prod)]["0_avg_quant"] += ((quant - group[(key_cust, key_prod)]["0_avg_quant"])/count_0_quant[(key_cust, key_prod)])



  #===================================================================
  #= the following scans process all grouping variables              =
  #= non-dependent grouping variables are processed in the same scan =
  #===================================================================

  #2th Scan:
  count_1_quant= collections.defaultdict(int)
  for (key_cust, key_prod) in group:
    for row in rows:
      #Grouping attributes:
      cust = row[0]
      prod = row[1]

      #Process Grouping Variable 1:
      quant = row[6]
      date = row[7]
      state = row[5]
      try:
        if group[(key_cust, key_prod)]["cust"] == cust and group[(key_cust, key_prod)]["prod"] == prod and quant > group[(key_cust, key_prod)]["0_avg_quant"]:
          if not group[(key_cust, key_prod)]["1_avg_quant"]:
            group[(key_cust, key_prod)]["1_avg_quant"] = quant
            count_1_quant[(key_cust, key_prod)] += 1
          else:
            count_1_quant[(key_cust, key_prod)] += 1
            group[(key_cust, key_prod)]["1_avg_quant"] += ((quant - group[(key_cust, key_prod)]["1_avg_quant"])/count_1_quant[(key_cust, key_prod)])
      except(TypeError):
        pass
      try:
        if group[(key_cust, key_prod)]["cust"] == cust and group[(key_cust, key_prod)]["prod"] == prod and quant > group[(key_cust, key_prod)]["0_avg_quant"]:
          if not group[(key_cust, key_prod)]["1_max_quant"]:
            group[(key_cust, key_prod)]["1_max_quant"] = quant
            group[(key_cust, key_prod)]["1.state"] = state
            group[(key_cust, key_prod)]["1.date"] = date
            group[(key_cust, key_prod)]["1.quant"] = quant
          else:
            if quant > group[(key_cust, key_prod)]["1_max_quant"]:
              group[(key_cust, key_prod)]["1_max_quant"] = quant
              group[(key_cust, key_prod)]["1.state"] = state
              group[(key_cust, key_prod)]["1.date"] = date
              group[(key_cust, key_prod)]["1.quant"] = quant
      except(TypeError):
        pass


  #===================================================
  #= formatter process and output the query result   =
  #===================================================
  columns_type = []
  columns_type.append(mf_type["cust"])
  columns_type.append(mf_type["prod"])
  columns_type.append(mf_type["0_avg_quant"])
  columns_type.append(mf_type["1_avg_quant"])
  columns_type.append(mf_type["1.quant"])
  columns_type.append(mf_type["1.state"])
  columns_type.append(mf_type["1.date"])

  row_formatter = []
  title_formatter = []
  for i, t in enumerate(columns_type):
    if t == "str" or t == "dt":
      row_formatter.append("{col" +str(i + 1) + ":<15}")
      title_formatter.append("{:<15}")
    elif t == "float":
      row_formatter.append("{col" +str(i + 1) + ":>15,.2f}")
      title_formatter.append("{:<15}")
    else:
      row_formatter.append("{col" +str(i + 1) + ":>15}")
      title_formatter.append("{:<15}")
  title_formatter = "|".join(title_formatter)
  row_formatter = "|".join(row_formatter)
  print(title_formatter.format("cust", "prod", "0_avg_quant", "1_avg_quant", "1.quant", "1.state", "1.date"))

  formatter = Formatter()
  for val in group.values():
    try:
      if val["1.quant"] == val["1_max_quant"]:
          data = {"col1": val["cust"], "col2": val["prod"], "col3": val["0_avg_quant"], "col4": val["1_avg_quant"], "col5": val["1.quant"], "col6": val["1.state"], "col7": str(val["1.date"])}
          print(formatter.format(row_formatter, **data))
    except(TypeError):
      pass


if __name__ == "__main__":
  query()