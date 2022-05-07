from OutputProcess.aggregateProcess import *
from CoreProcess.groupVariableProcess import *
from OutputProcess.formatter import *

def writeMFStructure(mf_structure, mf_type, global_indentation):
  """The function to write the script of mf_structure and group hashtable used for scanning"""

  structure = ("\n\n" + (" " * global_indentation) + "#====================================================================================\n")
  structure += ((" " * global_indentation) + "#= the data structure of mf_structure is hashtable                                  =\n")
  structure += ((" " * global_indentation) + "#= group is a hashtable with grouping attributes as keys and mf_structure as values =\n")
  structure += ((" " * global_indentation) + "#====================================================================================\n")

  structure += ((" " * global_indentation) + "mf_structure = ")
  structure += (str(mf_structure) + "\n")

  structure += ((" " * global_indentation) + "mf_type = ")
  structure += (str(mf_type) + "\n")

  structure += ((" " * global_indentation) + "group = collections.defaultdict(lambda: dict(mf_structure))\n\n")

  return structure

def writeFirstScan(V, schema, script, global_indentation, group_variable_fs):
  """The function to write the script of first initial scan"""

  # the function writes the script of the first scan, the first scan fill grouping attribute into the hashmap
  # and process the aggregation function of the grouping variable 0

  key_V = ["key_" + group_attr for group_attr in V]
  group_attr = "(" + ", ".join(key_V) + ")"
  F0 = [(f[0], f[1], f[2] )for f in group_variable_fs[0]]

  for f in F0:
    if "avg" in f[0]:
      script += ((" " * global_indentation) + "count_0_" +  f[1] + "= collections.defaultdict(int)\n")
  
  script += ((" " * global_indentation) + "for row in rows:\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "#Grouping attributes:\n")

  # extract grouping attributes (ex: cust, prod)
  for attr in V:
    if attr not in schema:
      raise(KeyError("Non-existent Column " + attr))
    script += ((" " * global_indentation) + "key_" + attr + " = row[" + schema[attr][0] + "]\n")

  # extract aggregated attributes (ex: quant)
  fun_attr_set = set(f[1] for f in F0)
  for attr in fun_attr_set:
    if attr not in schema:
      raise(KeyError("Non-existent Column " + attr))
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr][0] + "]\n")

  # first filling the grouping attributes, this process only works in first scan
  group_key = "group[" + group_attr + "]"
  script += ((" " * global_indentation) + "if not " + group_key + '["' + V[0] + '"]:\n')
  global_indentation += 2
  for v in V:
    script += ((" " * global_indentation) + group_key + '["' + v + '"]' + " = key_" + v + "\n")
  global_indentation -= 2


  for f in F0:
    if f[0] == "avg":
      script += avgScript(group_attr, f, "0", global_indentation, None)
    elif f[0] == "max":
      script += maxScript(group_attr, f, None, global_indentation, None)
    elif f[0] == "min":
      script += minScript(group_attr, f, None, global_indentation, None)
    elif f[0] == "count":
      script += countScript(group_attr, f, global_indentation, None)
    elif f[0] == "sum":
      script += sumScript(group_attr, f, global_indentation, None)
    else:
      raise (ValueError("Unvalid aggregation function " + f))


  return script

def writeGroupVariableScan(V, C, schema, to_be_scan, group_variable_fs, 
  group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate, script, global_indentation):
  """The function to write the script of scans of grouping variables"""

  # the function writes the script of the scan for the grouping variables, multiple variables may be processed at the
  # same scan if they have no dependence.
  
  key_V = ["key_" + group_attr for group_attr in V]
  group_attr = "(" + ", ".join(key_V) + ")"

  # check avg
  for group_variable in to_be_scan:
    for f in group_variable_fs[group_variable]:
      if "avg" in f[0]:
        script += ((" " * global_indentation) + "count_" + str(group_variable) + "_" + f[1] + "= collections.defaultdict(int)\n")
  
  

  script += ((" " * global_indentation) + "for " + group_attr + " in group:\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "for row in rows:\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "#Grouping attributes:\n")
  

  # single scan may process mutiple independent grouping variables
  for index, group_variable in enumerate(to_be_scan):
    if (index == 0):
      # extract grouping attributes (ex: cust, prod)
      for attr in V:
        script += ((" " * global_indentation) + attr + " = row[" + schema[attr][0] + "]\n")

    # parse condition and extract the attributes used in C
    processed_condition, such_that_attr = processCondition(V, C[group_variable - 1], group_attr, schema)
    
    script += (f"\n" + (" " * global_indentation) + f"#Process Grouping Variable {group_variable}:\n")

    # extract all required attributes for updating in the scan, there are 3 cases
    # case 1. extract aggregated attributes (From F), ex: "quant" of 1_sum_quant
    fun_attr_set = set(f[1] for f in group_variable_fs[group_variable]) 

    # case 2. extract attributed used in C (From C), ex: "state" of 1.state == "NY"
    required_attr_set = fun_attr_set.union(set(such_that_attr)) 

    # case 3. extract grouping variable's attrribute (those attributes are needed to updated during scans)
    # ex: "quant", "state", "year" of 'select cust, prod, 1.quant, 1.state, 1.year'
    required_attr_set = required_attr_set.union(set([attr.split(".")[1] for attr in group_variable_attrs[group_variable]]))

    for attr in required_attr_set:
      if attr not in schema:
        raise(KeyError("Non-existent Column " + attr))
      script += ((" " * global_indentation) + attr  + " = row[" + schema[attr][0] + "]\n")

    # if the grouping variable has no aggregation function, just apply the condition
    if not group_variable_fs[group_variable]:
      script += noAggregate(group_attr, group_variable_attrs[group_variable], global_indentation, processed_condition)
    
    # if it has aggregation functions, process them
    else:
      for f in group_variable_fs[group_variable]:
        if f[0] == "avg":
          script += avgScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
        elif f[0] == "max":
          script += maxScript(group_attr, f, group_variable_attrs_max_aggregate[group_variable], global_indentation, processed_condition)
        elif f[0] == "min":
          script += minScript(group_attr, f, group_variable_attrs_min_aggregate[group_variable], global_indentation, processed_condition)
        elif f[0] == "count":
          script += countScript(group_attr, f, global_indentation, processed_condition)
        elif f[0] == "sum":
          script += sumScript(group_attr, f, global_indentation, processed_condition)
        else:
          raise (ValueError("Unvalid aggregation function " + f))
  return script



def writeProject(S, G, schema, script, global_indentation):
  """The function to writhe the script of query result projection"""


  # the function writes the script of the results projections. it applies the formatter and the having condition

  # get the type
  script += ((" " * global_indentation) + "columns_type = []\n")
  columns_type = ""
  for i, s in enumerate(S):
    columns_type += ((" " * global_indentation) + 'columns_type.append(mf_type["' + s + '"])\n')

  script += columns_type

  # build formatter
  script += formatterScript(global_indentation)

  title = "print(title_formatter.format("
  for i, s in enumerate(S):
    if i == len(S) - 1:
      title += '"' + s + '"))\n'
    else:
      title += '"' + s + '", '
  script += ((" " * global_indentation) + title + "\n")

  script += ((" " * global_indentation) + "formatter = Formatter()\n")
  script += ((" " * global_indentation) + "for val in group.values():\n")
  global_indentation += 2

  all_output_attr = ""

  if len(G) and len(G[0]):
    all_output_attr += ((" " * global_indentation) + "try:\n")
    global_indentation += 2
    having = processHaving(G[0], schema)
    all_output_attr += ((" " * global_indentation) + "if " + having + ":\n")
    global_indentation += 2


  all_output_attr += ((" " * global_indentation) + "data = {") 
  is_date = False
  for i, s in enumerate(S):
    if "." in s:
      splitted = s.split(".")
      if schema[splitted[1]][1] == 'date' or schema[splitted[1]][1] == 'datetime':
        is_date = True
    if "_" in s:
      splitted = s.split("_")
      if schema[splitted[2]][1] == 'date' or schema[splitted[2]][1] == 'datetime':
        is_date = True

    if i != len(S) - 1:
      if is_date:
        all_output_attr += ('"col' + str(i + 1) + '": ' + 'str(val["' + s + '"]), ')
        is_date = False
      else:
        all_output_attr += ('"col' + str(i + 1) + '": ' + 'val["' + s + '"], ')
    else:
      if is_date:
        all_output_attr += ('"col' + str(i + 1) + '": ' + 'str(val["' + s + '"])}')
        is_date = False
      else:
        all_output_attr += ('"col' + str(i + 1) + '": ' + 'val["' + s + '"]}')
  
  all_output_attr += ("\n" + (" " * global_indentation) + "print(formatter.format(row_formatter, **data))\n")

  if len(G) and len(G[0]):
    global_indentation -= 4
    all_output_attr += ((" " * global_indentation) + "except(TypeError):\n")
    global_indentation += 2
    all_output_attr += ((" " * global_indentation) + "pass\n")
    
  global_indentation -= 8
  script += ((" " * global_indentation) + all_output_attr)
  
  return script