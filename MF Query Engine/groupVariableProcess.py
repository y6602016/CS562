import collections
from xml.sax.handler import DTDHandler

def processRel(N, F, C):
  group_variable_fs = collections.defaultdict(list)
  group_variable_conditions = collections.defaultdict(list)
  depend_map = collections.defaultdict(list)

  for i in range(1, int(N[0]) + 1):
    # process function
    for f in F:
      splitted = f.split("_")
      if splitted[0] == str(i):
        #("avg", "quant", "1_avg_quant")
        group_variable_fs[i].append((splitted[1], splitted[2], f))
    
    # process conditions
    condition = C[i - 1]
    new_string = []
    j = 0
    
    while j < len(condition):
      c = condition[j]
      if c.isdigit() and int(c) == i:
        j += 2
        continue
      else:
        if c.isdigit() and int(c) < i:
          depend_map[i].append(int(c))
        new_string.append(c)
        j += 1
    processed_condition = "".join(new_string)
    group_variable_conditions[i].append(processed_condition)
  
  return group_variable_fs, group_variable_conditions, depend_map




