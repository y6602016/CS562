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
  script += ((" " * global_indentation) + "group = collections.defaultdict(lambda:mf_structure)\n\n")

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

def writeScan(V, F, schema, script, global_indentation):

  F0 = []

  for f in F:
    splitted = f.split("_")
    if splitted[0] == "0":
      F0.append((splitted[1], splitted[2], f))
  
  if len(F0):
    for f in F0:
      if "avg" in f[0]:
        script += ((" " * global_indentation) + "count = collections.defaultdict(int)\n")
    script += ((" " * global_indentation) + "for row in cursor:\n")
    global_indentation += 2
    

    for attr in V:
      script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

    fun_attr_set = set(f[1] for f in F0)
    for attr in fun_attr_set:
      script += ((" " * global_indentation) + attr + " = row[" + schema[attr] + "]\n")

    t = "(" + ", ".join(V) + ")"

    s = "group[" + t + ']["'  + F0[0][2] + '"]'
    script += ((" " * global_indentation) + "if not " + s + ":\n")
    global_indentation += 2
    script += ((" " * global_indentation) + s + " = " + F0[0][1] + "\n")
    global_indentation -= 2 
    script += ((" " * global_indentation) + "else:\n")
    global_indentation += 2
    script += ((" " * global_indentation) + s + " += " + F0[0][1] + "\n")
    global_indentation -= 2
    s = "count[" + t + "]"
    script += ((" " * global_indentation) + s + " += 1\n")

    s = "group[" + t + ']["'  + F0[1][2] + '"]'
    script += ((" " * global_indentation) + "if not " + s + ":\n")
    global_indentation += 2
    script += ((" " * global_indentation) + s + " = " + F0[0][1] + "\n")
    global_indentation -= 2 
    script += ((" " * global_indentation) + "else:\n")
    global_indentation += 2
    script += ((" " * global_indentation) + s + " = max(" + F0[1][1] + ", " + s + ")\n\n")


  return script


# need fix
def writeProject(S, script, global_indentation):
  script += ((" " * global_indentation) + "for key, val in group.items():\n")
  global_indentation += 2
  script += ((" " * global_indentation) + "print(key[0], key[1], val['0_avg_quant'] / count[key], val['0_max_quant'])\n\n")

  return script