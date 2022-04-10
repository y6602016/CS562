import collections
import enum

def processRel(N, F):
  group_variable_fs = collections.defaultdict(list)
  depend_map = collections.defaultdict(list)

  for i in range(1, int(N[0]) + 1):
    # process function
    for f in F:
      splitted = f.split("_")
      if splitted[0] == str(i):
        #("avg", "quant", "1_avg_quant")
        group_variable_fs[i].append((splitted[1], splitted[2], f))
  
  return group_variable_fs, depend_map


def processCondition(V, group_variable, condition, group_attr, schema):
  # If the attribute is grouping attribute:
  # replace all ("grouping varable" + ".") with (group[(grouping attribute)][")
  # If the attribute is not a grouping attribute:
  # remove ("grouping varable" + ".")
  # ex: 1.cust==cust and 1.state=="NY"  ->  group[(cust, prod)]["cust==cust and state=="NY"


  new_processed = []
  such_that_attr = []
  i = 0
  while i < len(condition):
    if condition[i] == str(group_variable) and condition[i + 1] == ".":
      j = i + 2
      while j < len(condition) and condition[j].isalpha():
        j += 1
      i += 2
      word = condition[i:j]
      # word is a grouping attribute
      if word in V:
        new_processed.append(f'group[{group_attr}]["')
      elif word in schema:
        such_that_attr.append(word)
    new_processed.append(condition[i])
    i += 1

  processed = "".join(new_processed)
  # second, fill "] at the end of grouping attribute
  for group_variable in V:
    processed = processed.replace(f'["{str(group_variable)}', f'["{str(group_variable)}"]')
  return processed, such_that_attr
      
      




