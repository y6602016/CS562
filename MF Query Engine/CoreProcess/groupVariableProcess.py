import collections
import re

regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])?(T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?)?$'
match_iso8601 = re.compile(regex).match
def validate_iso8601(str_val):
    try:            
        if match_iso8601(str_val) is not None:
            return True
    except:
        pass
    return False


def processRel(N, C, F):
  """The function to parse grouping variable's aggregation functions used in S, C or G, and create the depending map"""

  # parse aggregation funcmtion into the format of {grouping variable: [('sum', 'quant', '0_sum_quant')]}
  group_variable_fs = collections.defaultdict(list)
  depend_map = collections.defaultdict(set)

  # process function
  for f in F:
    splitted = f.split("_")
    group_variable_fs[int(splitted[0])].append((splitted[1], splitted[2], f))

  for i in range(1, int(N[0]) + 1):
    # process conditions
    condition = C[i - 1]

    # j represent the previous grouping variable
    for j in range(1, i):
      # case 1, condition mentions previous grouping variable's attributes
      if str(j) + "." in condition:
        depend_map[i].add(j)

      # case 2, condition mentions previous grouping variable's aggregation function
      for pre_function_list in group_variable_fs[j]:
        if pre_function_list[2] in condition:
          depend_map[i].add(j)

  return group_variable_fs, depend_map


def processCondition(V, condition, group_attr, schema):
  """The function to parse each grouping variable's condition statement, parse mf_structure key and special data type"""

  # parse condition statement with hashtable key and convert specail data type such as "date"
  # before processed:
  # 1.cust == cust and 1.prod == prod and 1.date > 2019-09-30 and 1.date < 2019-12-01
  # after processed: 
  # group[(key_cust, key_prod)]["cust"] == cust and group[(key_cust, key_prod)]["prod"] == prod and date > date.fromisoformat("2019-09-30") and date < date.fromisoformat("2019-12-01")
  
  new_processed = []

  # such_that_attr containse the attributes used in condition statement, ex: "date" in 1.date > 2019-09-30 and 1.date < 2019-12-01
  # theses attributes may not be in mf_structure
  such_that_attr = []

  group_variable = condition[0]
  mf_check = ""
  for i, v in enumerate(V):
    mf_check += (group_variable + "." + v + " == " + v)
    if i != len(V) - 1:
      mf_check += " and "

  is_emf = False
  if mf_check not in condition or (len(mf_check) + 1 < len(condition) and condition[len(mf_check) + 1] != "and"):
    is_emf = True


  splitted = condition.split(" ")
  special_type = None
  special_type_index = -1
  emf_process_index = -1

  for i, word in enumerate(splitted):
    temp_word = word
    word = word.replace("(", "")
    word = word.replace(")", "")
    processed = ""
    if "." in word:
      attr = word.split(".")[1]
      if attr not in schema:
        raise(KeyError("Non-existent Column " + attr))
      if attr in V:
        processed = f'group[{group_attr}]["' + attr + '"]'
        if is_emf:
          emf_process_index = i
      # the attributes of grouping variable appear in condition statement should be recorded
      # such that we can update the attributes in the scan
      elif attr in schema: 
        such_that_attr.append(attr)
        processed = attr
        if schema[attr][1] == 'date': # if the attribute is special type, mark it then process it later
          special_type = 'date'
          special_type_index = i
        elif schema[attr][1] == 'datetime':
          special_type = 'datetime'
          special_type_index = i
    elif "_" in word:
      func = word.split("_")[1]
      if func not in ("sum", "max", "min", "avg", "count"):
        raise(ValueError("Unvalid aggregation function " + func))
      attr = word.split("_")[2]
      if attr not in schema:
        raise(KeyError("Non-existent Column " + attr))
      processed = f'group[{group_attr}]["' + word + '"]'
    else:
      # if not a string, not a constant number, not an operator, it's a attribute, check it's in column ot not
      if '"' not in word and "'" not in word and not word.isdigit() and word not in "!@#$%^&*()_-+=={[]}<><=>=!=/andor" and not validate_iso8601(word):
        if word not in schema:
          if not validate_iso8601(word):
            raise (ValueError("Invalid time format " + word))
          else:
            raise(KeyError("Non-existent Column " + word))
      if special_type_index > -1 and i == special_type_index + 2: # process the special type object to be compared with
        if special_type == 'date':
          processed = f'date.fromisoformat("' + word + '")'
        elif special_type == 'datetime':
          processed = f'datetime.fromisoformat("' + word + '")'
        special_type_index = -1
        special_type = None
      elif emf_process_index > -1:
        if i == emf_process_index + 1 and (word == ">" or word == "<"):
          processed = "<" if word == ">" else ">"
          if splitted[i + 2] == "+" or splitted[i + 2] == "-":
            splitted[i + 2] = "-" if splitted[i + 2] == "+" else "+"
          elif splitted[i + 2] == "*" or splitted[i + 2] == "/":
            splitted[i + 2] = "/" if splitted[i + 2] == "*" else "/"
          emf_process_index = - 1
        elif i == emf_process_index + 1 and (word == ">=" or word == "<="):
          processed = "<=" if word == ">=" else ">="
          if splitted[i + 2] == "+" or splitted[i + 2] == "-":
            splitted[i + 2] = "-" if splitted[i + 2] == "+" else "+"
          elif splitted[i + 2] == "*" or splitted[i + 2] == "/":
            splitted[i + 2] = "/" if splitted[i + 2] == "*" else "/"
          emf_process_index = - 1
        elif splitted[emf_process_index + 1] == "==" and i == emf_process_index + 3 and (word == "+" or word == "-"):
          processed = "-" if word == "+" else "+"
          emf_process_index = - 1
        elif splitted[emf_process_index + 1] == "==" and i == emf_process_index + 3 and (word == "*" or word == "/"):
          processed = "/" if word == "*" else "/"
          emf_process_index = - 1
        else:
          processed = word
      else:
        processed = word

    
    for char in temp_word:
      if char == "(":
        processed = "(" + processed
        
      if char == ")":
        processed = processed + ")"

    new_processed.append(processed)

  processed = " ".join(new_processed)
  return processed, such_that_attr


def processHaving(having, schema):
  """The function to parse having condition statement, parse mf_structure key and special data type"""

  # parse condition statement with hashtable key and convert specail data type such as "date"
  # before processed:
  # 1_sum_quant * 10 > 0_sum_quant and 1.quant == 1_min_quant and 1_min_quant > 150
  # after processed:
  # val["1_sum_quant"] * 10 > val["0_sum_quant"] and val["1.quant"] == val["1_min_quant"] and val["1_min_quant"] > 150


  new_processed = []
  splitted = having.split(" ")
  special_type_index = -1
  special_type = None

  for i, word in enumerate(splitted):
    temp_word = word
    word = word.replace("(", "")
    word = word.replace(")", "")
    processed = ""
    if "." in word:
      processed = f'val["' + word + '"]'
      attr = word.split(".")[1]
      if attr not in schema:
        raise(KeyError("Non-existent Column " + attr))
      if schema[attr][1] == 'date': # if the attribute is special type, mark it then process it later
          special_type = 'date'
          special_type_index = i
      elif schema[attr][1] == 'datetime':
        special_type = 'datetime'
        special_type_index = i
    elif "_" in word:
      func = word.split("_")[1]
      if func not in ("sum", "max", "min", "avg", "count"):
        raise(ValueError("Unvalid aggregation function " + func))
      attr = word.split("_")[2]
      if attr not in schema:
        raise(KeyError("Non-existent Column " + attr))
      processed = f'val["' + word + '"]'
    else:
      # if not a string, not a constant number, not an operator, it's a attribute, check it's in column ot not
      if '"' not in word and "'" not in word and not word.isdigit() and word not in "!@#$%^&*()_-+=={[]}<><=>=!=/andor":
        if word not in schema:
          if not validate_iso8601(word):
            raise (ValueError("Invalid time format " + word))
          else:
            raise(KeyError("Non-existent Column " + word))
      if special_type_index > -1 and i == special_type_index + 2:
        if special_type == 'date':
          processed = f'date.fromisoformat("' + word + '")'
        elif special_type == 'datetime':
          processed = f'datetime.fromisoformat("' + word + '")'
        special_type = None
        special_type_index = -1
      else:
        processed = word

    for char in temp_word:
      if char == "(":
        processed = "(" + processed
        
      if char == ")":
        processed = processed + ")"

    new_processed.append(processed)

  processed = " ".join(new_processed)
  return processed


def processAttr(mf_structure, N, C, G):
  """The function to process grouping variable's attributes used in S, C or G"""

  # if S contains gv's attr, it means the gv is narrowed to a single tuple
  # we need to find out where the narrowing occurs. It may occur in C or in G
  # ex: 
  # in C: such that 1.length == 0_max_length
  # in G: having 1.length == 1_max_length
  # if in C, update the attrs in noAggregate function of the scan
  # if in G, update the attrs in the corresponding aggregate functions(max or min)

  group_variable_attrs = collections.defaultdict(list) # for the case in C
  group_variable_attrs_max_aggregate = collections.defaultdict(list) # for the case in G
  group_variable_attrs_min_aggregate = collections.defaultdict(list) # for the case in G

  all_attr_set = set([attr for attr in mf_structure if "." in attr])
  
  for i in range(1, int(N[0]) + 1):
    attr_list = []
    prefix = str(i) + "."

    for attr in all_attr_set:
      # extract the group variable's attr from mf_structure
      if prefix in attr:
        attr_list.append(attr)
    
    for attr in attr_list:
      all_attr_set.remove(attr)

    # check group variable in C or not
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
          # if group variable's attrs is narrowed in C
          if "max" in compared or "min" in compared:
            group_variable_attrs[i] = attr_list

    
    # check group variable in G or not. if not, it's in C
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

            # if group variable's attrs is narrowed in G
            if "max" in compared:
              group_variable_attrs_max_aggregate[i] = attr_list
              group_variable_attrs[i] = attr_list
              break
            elif "min" in compared:
              group_variable_attrs_min_aggregate[i] = attr_list
              group_variable_attrs[i] = attr_list
              break

  return group_variable_attrs, group_variable_attrs_max_aggregate, group_variable_attrs_min_aggregate


      
    



    

      
      




