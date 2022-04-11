from aggregateProcess import *
from groupVariableProcess import processCondition

def writeMFStructure(mf_structure, script, global_indentation):
  script += ((" " * global_indentation) + "mf_structure = ")
  
  script += (str(mf_structure) + "\n")

  script += ((" " * global_indentation) + "group = collections.defaultdict(lambda: dict(mf_structure))\n\n")

  return script

def writeGroupAttrIndex(V, schema, script, global_indentation):
  script += ((" " * global_indentation) + "group_attr_index = ")
  group_attr_index = list()
  for i, attr in enumerate(schema):
    if attr in V:
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

  # extract grouping attributes (ex: cust, prod)
  for attr in V:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

  # extract aggregated attributes (ex: quant)
  fun_attr_set = set(f[1] for f in F0)
  for attr in fun_attr_set:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

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
      script += maxScript(group_attr, f, global_indentation, None)
    elif f[0] == "min":
      script += minScript(group_attr, f, global_indentation, None)
    elif f[0] == "count":
      script += countScript(group_attr, f, global_indentation, None)
    elif f[0] == "sum":
      script += sumScript(group_attr, f, global_indentation, None)


  return script

def writeGroupVariableScan(V, C, schema, to_be_scan, group_variable_fs, depend_fun, script, global_indentation):
  
  script += ((" " * global_indentation) + "cursor.execute(query)\n\n")

  # check avg
  for group_variable in to_be_scan:
    for f in group_variable_fs[group_variable]:
      if "avg" in f[0]:
        script += ((" " * global_indentation) + "count_" + str(group_variable) + "_" + f[1] + "= collections.defaultdict(int)\n")

  script += ((" " * global_indentation) + "for row in cursor:\n")
  global_indentation += 2

  group_attr = "(" + ", ".join(V) + ")"

  # single scan may process mutiple independent grouping variables
  for index, group_variable in enumerate(to_be_scan):

    if (index == 0):
      # extract grouping attributes (ex: cust, prod)
      for attr in V:
        script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

    processed_condition, such_that_attr = processCondition(V, group_variable, C[group_variable - 1], group_attr, schema, depend_fun)

    # extract aggregated attributes and attr used in such that clause (ex: quant)
    script += (f"\n\n" + (" " * global_indentation) + f"#Process Grouping Variable {group_variable}:\n")
    fun_attr_set = set(f[1] for f in group_variable_fs[group_variable])
    required_attr_set = fun_attr_set.union(set(such_that_attr))
    for attr in required_attr_set:
      script += ((" " * global_indentation) + attr  + " = row[" + schema[attr] + "]\n")

    

    for f in group_variable_fs[group_variable]:
      if f[0] == "avg":
        script += avgScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
      elif f[0] == "max":
        script += maxScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
      elif f[0] == "min":
        script += minScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
      elif f[0] == "count":
        script += countScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
      elif f[0] == "sum":
        script += sumScript(group_attr, f, str(group_variable), global_indentation, processed_condition)
  
  return script



def writeProject(S, script, global_indentation):
  script += ("\n\n" + (" " * global_indentation) + "for val in group.values():\n")
  global_indentation += 2
  all_output_attr = ""
  for i, s in enumerate(S):
    if i != len(S):
      all_output_attr += 'val["' + s + '"], '
    else:
      all_output_attr += 'val["' + s + '"]'
  script += ((" " * global_indentation) + "print(" + all_output_attr + ")\n\n")

  return script