from aggregateProcess import *
from groupVariableProcess import *

def writeMFStructure(mf_structure, script, global_indentation):
  script += ((" " * global_indentation) + "mf_structure = ")
  
  script += (str(mf_structure) + "\n")

  script += ((" " * global_indentation) + "group = collections.defaultdict(lambda: dict(mf_structure))\n\n")

  return script

def writeGroupAttrIndex(V, schema, script, global_indentation):
  script += ((" " * global_indentation) + "group_attr_index = ")
  group_attr_index = list()
  for i, attr in enumerate(schema):
    if attr[0] in V:
      group_attr_index.append(i)
  script += (str(group_attr_index) + "\n\n")
  return script, group_attr_index

def writeFirstScan(V, F, schema, script, global_indentation):
  # ex:
  # F = ["0_avg_quant", "0_max_quant"]
  # F0 = [("avg", "quant", "0_avg_quant"), ("max", "quant", "0_max_quant")]
  F0 = []
  for f in F:
    splitted = f.split("_")
    if splitted[0] == "0":
      F0.append((splitted[1], splitted[2], f))
  
  group_attr = "(" + ", ".join(V) + ")"


  for f in F0:
    if "avg" in f[0]:
      script += ((" " * global_indentation) + "count_0_" +  f[1] + "= collections.defaultdict(int)\n")
  
  script += ((" " * global_indentation) + "for row in cursor:\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "#Grouping attributes:\n")

  # extract grouping attributes (ex: cust, prod)
  for attr in V:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr][0] + "]\n")

  # extract aggregated attributes (ex: quant)
  fun_attr_set = set(f[1] for f in F0)
  for attr in fun_attr_set:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr][0] + "]\n")

  # first filling the grouping attributes, this process only works in first scan
  group_key = "group[" + group_attr + "]"
  script += ((" " * global_indentation) + "if not " + group_key + '["' + V[0] + '"]:\n')
  global_indentation += 2
  for v in V:
    script += ((" " * global_indentation) + group_key + '["' + v + '"]' + " = " + v + "\n")
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


  return script

def writeGroupVariableScan(V, C, schema, to_be_scan, group_variable_fs, depend_fun, 
  group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate, script, global_indentation):
  
  script += ((" " * global_indentation) + "cursor.execute(query)\n\n")

  # check avg
  for group_variable in to_be_scan:
    for f in group_variable_fs[group_variable]:
      if "avg" in f[0]:
        script += ((" " * global_indentation) + "count_" + str(group_variable) + "_" + f[1] + "= collections.defaultdict(int)\n")

  script += ((" " * global_indentation) + "for row in cursor:\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "#Grouping attributes:\n")
  group_attr = "(" + ", ".join(V) + ")"

  # single scan may process mutiple independent grouping variables
  for index, group_variable in enumerate(to_be_scan):
    if (index == 0):
      # extract grouping attributes (ex: cust, prod)
      for attr in V:
        script += ((" " * global_indentation) + attr + " = row[" + schema[attr][0] + "]\n")

    processed_condition, such_that_attr = processCondition(V, C[group_variable - 1], group_attr, schema)
    
    # extract aggregated attributes and attr used in such that clause (ex: quant)
    # attrs can be from F, C, S

    script += (f"\n" + (" " * global_indentation) + f"#Process Grouping Variable {group_variable}:\n")
    fun_attr_set = set(f[1] for f in group_variable_fs[group_variable])
    required_attr_set = fun_attr_set.union(set(such_that_attr))
    required_attr_set = required_attr_set.union(set([attr.split(".")[1] for attr in group_variable_attrs[group_variable]]))
    for attr in required_attr_set:
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
    
  return script



def writeProject(S, G, mf_structure, schema, script, global_indentation):
  # get the type
  script += ("\n\n" + (" " * global_indentation) + "columns_type = []\n")
  script += ((" " * global_indentation) + "for val in group.values():\n")
  global_indentation += 2
  columns_type = ""
  for i, s in enumerate(S):
    columns_type += ((" " * global_indentation) + 'columns_type.append(type(val["' + s + '"]))\n')
  script += (columns_type + (" " * global_indentation) + "break\n\n")
  global_indentation -= 2

  # build formatter
  type_formater = ((" " * global_indentation) + 'row_formatter = []\n')
  type_formater += ((" " * global_indentation) + 'title_formatter = []\n')
  type_formater += ((" " * global_indentation) + 'date_index = []\n')
  type_formater += ((" " * global_indentation) + "for i, t in enumerate(columns_type):\n")
  global_indentation += 2
  type_formater += ((" " * global_indentation) + "if t == str or t == dt:\n")
  global_indentation += 2
  type_formater += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":<15}")\n')
  type_formater += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  type_formater += ((" " * global_indentation) + "elif t == float:\n")
  global_indentation += 2
  type_formater += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":>15,.2f}")\n')
  type_formater += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  type_formater += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  type_formater += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":>15}")\n')
  type_formater += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  global_indentation -= 2

  type_formater += ((" " * global_indentation) + 'title_formatter = "|".join(title_formatter)\n')
  type_formater += ((" " * global_indentation) + 'row_formatter = "|".join(row_formatter)\n')

  script += type_formater


  title = "print(title_formatter.format("
  for i, s in enumerate(S):
    if i == len(S) - 1:
      title += '"' + s + '"))\n'
    else:
      title += '"' + s + '", '
  script += ((" " * global_indentation) + title + "\n")

  script += ((" " * global_indentation) + "formater = Formatter()\n")
  script += ((" " * global_indentation) + "for val in group.values():\n")
  global_indentation += 2
  
  if G[0]:
    having = processHaving(G[0], schema)
    script += ((" " * global_indentation) + "if " + having + ":\n")
    global_indentation += 2

  all_output_attr = "data = {"
  is_date = False
  for i, s in enumerate(S):
    if "." in s:
      splitted = s.split(".")
      if schema[splitted[1]][1] == 'date':
        is_date = True
    if "_" in s:
      splitted = s.split("_")
      if schema[splitted[2]][1] == 'date':
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

  all_output_attr += ("\n" + (" " * global_indentation) + "print(formater.format(row_formatter, **data))\n")

  script += ((" " * global_indentation) + all_output_attr + "\n\n")

  return script