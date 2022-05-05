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
    mf_structure = {'cust': None, 'prod': None, 'year': None, '1_avg_quant': None, '2_avg_quant': None}
    mf_type = {'cust': 'str', 'prod': 'str', 'year': 'int', '1_avg_quant': 'float', '2_avg_quant': 'float'}
    group = collections.defaultdict(lambda: dict(mf_structure))



    #=====================================================
    #= the first scan to fill all grouping attributes    =
    #= and aggregatation function of grouping variable_0 =
    #=====================================================

    #1th Scan:
    for row in rows:
      #Grouping attributes:
      key_year = row[4]
      key_prod = row[1]
      key_cust = row[0]
      if not group[(key_year, key_prod, key_cust)]["year"]:
        group[(key_year, key_prod, key_cust)]["year"] = key_year
        group[(key_year, key_prod, key_cust)]["prod"] = key_prod
        group[(key_year, key_prod, key_cust)]["cust"] = key_cust



    #===================================================================
    #= the following scans process all grouping variables              =
    #= non-dependent grouping variables are processed in the same scan =
    #===================================================================

    #2th Scan:
    count_1_quant= collections.defaultdict(int)
    count_2_quant= collections.defaultdict(int)
    for (key_year, key_prod, key_cust) in group:
      for row in rows:
        #Grouping attributes:
        year = row[4]
        prod = row[1]
        cust = row[0]

        #Process Grouping Variable 1:
        quant = row[6]
        try:
          if group[(key_year, key_prod, key_cust)]["cust"] == cust and group[(key_year, key_prod, key_cust)]["prod"] == prod and group[(key_year, key_prod, key_cust)]["year"] == year:
            if not group[(key_year, key_prod, key_cust)]["1_avg_quant"]:
              group[(key_year, key_prod, key_cust)]["1_avg_quant"] = quant
              count_1_quant[(key_year, key_prod, key_cust)] += 1
            else:
              count_1_quant[(key_year, key_prod, key_cust)] += 1
              group[(key_year, key_prod, key_cust)]["1_avg_quant"] += ((quant - group[(key_year, key_prod, key_cust)]["1_avg_quant"])/count_1_quant[(key_year, key_prod, key_cust)])
        except(TypeError):
          pass

        #Process Grouping Variable 2:
        quant = row[6]
        try:
          if group[(key_year, key_prod, key_cust)]["cust"] == cust and group[(key_year, key_prod, key_cust)]["prod"] == prod and group[(key_year, key_prod, key_cust)]["year"] != year:
            if not group[(key_year, key_prod, key_cust)]["2_avg_quant"]:
              group[(key_year, key_prod, key_cust)]["2_avg_quant"] = quant
              count_2_quant[(key_year, key_prod, key_cust)] += 1
            else:
              count_2_quant[(key_year, key_prod, key_cust)] += 1
              group[(key_year, key_prod, key_cust)]["2_avg_quant"] += ((quant - group[(key_year, key_prod, key_cust)]["2_avg_quant"])/count_2_quant[(key_year, key_prod, key_cust)])
        except(TypeError):
          pass


    #===================================================
    #= formatter process and output the query result   =
    #===================================================
    columns_type = []
    columns_type.append(mf_type["cust"])
    columns_type.append(mf_type["prod"])
    columns_type.append(mf_type["year"])
    columns_type.append(mf_type["1_avg_quant"])
    columns_type.append(mf_type["2_avg_quant"])

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
    print(title_formatter.format("cust", "prod", "year", "1_avg_quant", "2_avg_quant"))

    formatter = Formatter()
    for val in group.values():
      data = {"col1": val["cust"], "col2": val["prod"], "col3": val["year"], "col4": val["1_avg_quant"], "col5": val["2_avg_quant"]}
      print(formatter.format(row_formatter, **data))

  except (Exception, psycopg2.DatabaseError) as error:
    print("Error detected:")
    print(error)
  finally:
    if conn is not None:
      conn.close()


if __name__ == "__main__":
  query()
