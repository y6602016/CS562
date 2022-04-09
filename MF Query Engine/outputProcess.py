from aggregateProcess import *


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
      script += ((" " * global_indentation) + "count_" +  f[1] + "= collections.defaultdict(int)\n")
  
  script += ((" " * global_indentation) + "for row in cursor:\n")
  global_indentation += 2

  for attr in V:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

  fun_attr_set = set(f[1] for f in F0)
  for attr in fun_attr_set:
    script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

  group_key = "group[" + group_attr + "]"
  script += ((" " * global_indentation) + "if not " + group_key + '["' + V[0] + '"]:\n')
  global_indentation += 2
  for v in V:
    script += ((" " * global_indentation) + group_key + '["' + v + '"]' + " = " + v + "\n")
  global_indentation -= 2

  for f in F0:
    if f[0] == "avg":
      script += avgScript(group_attr, f, global_indentation, None)
    elif f[0] == "max":
      script += maxScript(group_attr, f, global_indentation, None)
    elif f[0] == "min":
      script += minScript(group_attr, f, global_indentation, None)
    elif f[0] == "count":
      script += countScript(group_attr, f, global_indentation, None)
    elif f[0] == "sum":
      script += sumScript(group_attr, f, global_indentation, None)


  return script


# need fix
def writeProject(S, script, global_indentation):
  script += ((" " * global_indentation) + "for val in group.values():\n")
  global_indentation += 2
  all_output_attr = ""
  for i, s in enumerate(S):
    if i != len(S):
      all_output_attr += 'val["' + s + '"], '
    else:
      all_output_attr += 'val["' + s + '"]'
  script += ((" " * global_indentation) + "print(" + all_output_attr + ")\n\n")

  return script