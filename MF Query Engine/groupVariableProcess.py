import collections

def processRel(N, C, F):
  group_variable_fs = collections.defaultdict(list)
  depend_map = collections.defaultdict(list)
  depend_fun = collections.defaultdict(list)

  # process function
  for f in F:
    splitted = f.split("_")
    group_variable_fs[int(splitted[0])].append((splitted[1], splitted[2], f))

  for i in range(1, int(N[0]) + 1):
    # process conditions
    condition = C[i - 1]
    j = 0

    while j < len(condition):
      c = condition[j]
      if c.isdigit() and int(c) == i:
        j += 2
        continue
      else:
        # if c.isdigit() and int(c) < i and int(c) != 0:
        #   depend_map[i].append(int(c))
        if c.isdigit() and int(c) < i:
          if int(c) != 0:
            depend_map[i].append(int(c))
          n = j + 1
          while n < len(condition) and (condition[n].isalpha() or condition[n] == "_"):
            n += 1
          func = condition[j:n]
          if func in F:
            depend_fun[i].append(func)
        j += 1

  return group_variable_fs, depend_map, depend_fun


def processCondition(V, group_variable, condition, group_attr, schema, depend_fun):
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

  # second, fill "] at the end of grouping attribute and depending aggregation functions
  for attr in V:
    processed = processed.replace(f'["{str(attr)}', f'["{str(attr)}"]')
  
  for fun in depend_fun[group_variable]:
    processed = processed.replace(fun, f'group[{group_attr}]["{fun}"]')

  return processed, such_that_attr
      
      




