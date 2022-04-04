from aggregateProcess import *


def writeMFStructure(mf_structure, script, global_indentation):
  # mf_structure = str(mf_structure)[1:-1].split(", ")

  script += ((" " * global_indentation) + "mf_structure = ")
  
  script += (str(mf_structure) + "\n")
  
  # global_indentation += 2

  # for i, item in enumerate(mf_structure):
  #   if i != len(mf_structure) - 1:
  #     script += ((" " * global_indentation) + item + ",\n")
  #   else:
  #     script += ((" " * global_indentation) + item + "\n")

  # global_indentation -= 2
  # script += ((" " * global_indentation) + "}\n")
  script += ((" " * global_indentation) + "group = collections.defaultdict(lambda: dict(mf_structure))\n\n")

  return script

def writeGroupAttrIndex(V, schema, script, global_indentation):
  script += ((" " * global_indentation) + "group_attr_index = ")
  group_attr_index = list()
  for i, attr in enumerate(schema):
    if attr in V:
      group_attr_index.append(i)
  script += (str(group_attr_index) + "\n\n")
  # for i, attr in enumerate(group_attr):
  #   if i != len(group_attr) - 1:
  #     script += (str(attr) + ", ")
  #   else:
  #     script += (str(attr) + ")")
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

  if not len(F0):
    return script


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
  script += ((" " * global_indentation) + "for key, val in group.items():\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "print(key[0], key[1], val['0_avg_quant'], val['0_max_quant'])\n\n")

  return script