import psycopg2
import collections
from Config.config import config
from datetime import date, datetime
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
  try:
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
    mf_structure = {'cust': None, 'prod': None, '1.quant': None, '1.state': None, '1.date': None, '0_sum_quant': None, '1_min_quant': None, '1_sum_quant': None}
    mf_type = {'cust': 'str', 'prod': 'str', '1.quant': 'int', '1.state': 'str', '1.date': 'date', '0_sum_quant': 'int', '1_min_quant': 'int', '1_sum_quant': 'int'}
    group = collections.defaultdict(lambda: dict(mf_structure))



    #=====================================================
    #= the first scan to fill all grouping attributes    =
    #= and aggregatation function of grouping variable_0 =
    #=====================================================

    #1th Scan:
    for row in rows:
      #Grouping attributes:
      key_cust = row[0]
      key_prod = row[1]
      quant = row[6]
      if not group[(key_cust, key_prod)]["cust"]:
        group[(key_cust, key_prod)]["cust"] = key_cust
        group[(key_cust, key_prod)]["prod"] = key_prod
      if not group[(key_cust, key_prod)]["0_sum_quant"]:
        group[(key_cust, key_prod)]["0_sum_quant"] = quant
      else:
        group[(key_cust, key_prod)]["0_sum_quant"] += quant



    #===================================================================
    #= the following scans process all grouping variables              =
    #= non-dependent grouping variables are processed in the same scan =
    #===================================================================

    #2th Scan:
    for (key_cust, key_prod) in group:
      for row in rows:
        #Grouping attributes:
        cust = row[0]
        prod = row[1]

        #Process Grouping Variable 1:
        quant = row[6]
        state = row[5]
        date = row[7]
        try:
          if group[(key_cust, key_prod)]["cust"] == cust and group[(key_cust, key_prod)]["prod"] == prod and date > date.fromisoformat("2019-05-31") and date < date.fromisoformat("2019-09-01"):
            if not group[(key_cust, key_prod)]["1_min_quant"]:
              group[(key_cust, key_prod)]["1_min_quant"] = quant
              group[(key_cust, key_prod)]["1.quant"] = quant
              group[(key_cust, key_prod)]["1.date"] = date
              group[(key_cust, key_prod)]["1.state"] = state
            else:
              if quant < group[(key_cust, key_prod)]["1_min_quant"]:
                group[(key_cust, key_prod)]["1_min_quant"] = quant
                group[(key_cust, key_prod)]["1.quant"] = quant
                group[(key_cust, key_prod)]["1.date"] = date
                group[(key_cust, key_prod)]["1.state"] = state
        except(TypeError):
          pass
        try:
          if group[(key_cust, key_prod)]["cust"] == cust and group[(key_cust, key_prod)]["prod"] == prod and date > date.fromisoformat("2019-05-31") and date < date.fromisoformat("2019-09-01"):
            if not group[(key_cust, key_prod)]["1_sum_quant"]:
              group[(key_cust, key_prod)]["1_sum_quant"] = quant
            else:
              group[(key_cust, key_prod)]["1_sum_quant"] += quant
        except(TypeError):
          pass


    #===================================================
    #= formatter process and output the query result   =
    #===================================================
    columns_type = []
    columns_type.append(mf_type["cust"])
    columns_type.append(mf_type["prod"])
    columns_type.append(mf_type["1.quant"])
    columns_type.append(mf_type["1.state"])
    columns_type.append(mf_type["1.date"])

    row_formatter = []
    title_formatter = []
    for i, t in enumerate(columns_type):
      if t == "str" or t == "date" or t == "datetime":
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
    print(title_formatter.format("cust", "prod", "1.quant", "1.state", "1.date"))

    formatter = Formatter()
    for val in group.values():
      try:
        if val["1_sum_quant"] * 10 > val["0_sum_quant"] and val["1.quant"] == val["1_min_quant"] and val["1_min_quant"] > 150:
          data = {"col1": val["cust"], "col2": val["prod"], "col3": val["1.quant"], "col4": val["1.state"], "col5": str(val["1.date"])}
          print(formatter.format(row_formatter, **data))
      except(TypeError):
        pass

  except (Exception, psycopg2.DatabaseError) as error:
    print("Error detected:")
    print(error)
  finally:
    if conn is not None:
      conn.close()


if __name__ == "__main__":
  query()
