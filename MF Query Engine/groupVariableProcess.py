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
    if condition[i] == str(group_variable) and i + 1 < len(condition) and condition[i + 1] == ".":
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


def processAttr(S, N, V, C, G):
  # if S contains gv's attr, it means the gv is narrowed to a single tuple
  # we need to find out where the narrowing occurs. It may in C or in G
  # ex: 
  # in C: such that 1.length == 0_max_length
  # in G: having 1.length == 1_max_length
  # if in C, update the attrs in noAggregate function of the scan
  # if in G, update the attrs in the corresponding aggregate functions(max or min)

  group_variable_attrs = collections.defaultdict(list) # for the case in C
  group_variable_attrs_max_aggregate = collections.defaultdict(list) # for the case in G
  group_variable_attrs_min_aggregate = collections.defaultdict(list) # for the case in G

  project_set = set(S)

  for grout_attr in V:
    project_set.remove(grout_attr)

  for i in range(1, int(N[0]) + 1):
    attr_list = []
    prefix = str(i) + "."

    for attr in project_set:
      # extract the gv's attr from S
      if prefix in attr:
        attr_list.append(attr)
    
    for attr in attr_list:
      project_set.remove(attr)
      
    # check gv in C or not
    c = C[i - 1]
    for attr in attr_list:
      to_find_string = attr + " == "
      index = c.find(to_find_string)
      if index > -1:
          start = index + len(to_find_string)
          end = start + 1
          while end < len(c) and c[end] != " ":
            end += 1
          compared = c[start:end]

          # if gv's attrs is narrowed in C
          if "max" in compared or "min" in compared:
            group_variable_attrs[i] = attr_list
    
    # check gv in G or not if not in C
    if not group_variable_attrs[i]:
      for attr in attr_list:
        to_find_string = attr + " == "
        having = G[0]
        index = having.find(to_find_string)
        if index > -1:
            start = index + len(to_find_string)
            end = start + 1
            while end < len(having) and having[end] != " ":
              end += 1
            compared = having[start:end]

            # if gv's attrs is narrowed in G
            if "max" in compared:
              group_variable_attrs_max_aggregate[i] = attr_list
              group_variable_attrs[i] = attr_list
              break
            elif "min" in compared:
              group_variable_attrs_min_aggregate[i] = attr_list
              group_variable_attrs[i] = attr_list
              break

  return group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate


      
    



    

      
      




