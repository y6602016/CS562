def avgScript(group_attr, func, group_variable, global_indentation, condition):
  """write the script of average aggregation function"""

  avg = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  count_key = "count_" + group_variable + "_" + func[1] + "[" + group_attr + "]"

  if condition:
    avg += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2

  avg += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  avg += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  avg += ((" " * global_indentation) + count_key + " += 1\n")
  global_indentation -= 2 

  avg += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  avg += ((" " * global_indentation) + count_key + " += 1\n")
  avg += ((" " * global_indentation) + group_key + " += ((" + func[1]  + " - " + group_key + ")/" + count_key + ")\n")
  
  return avg


def maxScript(group_attr, func, group_variable_attrs_max_aggregate, global_indentation, condition):
  """write the script of max aggregation function"""
  
  max = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'

  if condition:
    max += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  max += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  if group_variable_attrs_max_aggregate:
    for attr in group_variable_attrs_max_aggregate:
      group_attr_key = "group[" + group_attr + ']["'  + attr + '"]'
      max += ((" " * global_indentation) + group_attr_key + " = " + attr.split(".")[1] + "\n")
  global_indentation -= 2 
  max += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  max += ((" " * global_indentation) + "if " + func[1] + " > " + group_key + ":\n")
  global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = " + func[1] + "\n")
  if group_variable_attrs_max_aggregate:
    for attr in group_variable_attrs_max_aggregate:
      group_attr_key = "group[" + group_attr + ']["'  + attr + '"]'
      max += ((" " * global_indentation) + group_attr_key + " = " + attr.split(".")[1] + "\n")

  return max


def minScript(group_attr, func, group_variable_attrs_min_aggregate, global_indentation, condition):
  """write the script of min aggregation function"""
  
  min = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  
  if condition:
    min += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  min += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  if group_variable_attrs_min_aggregate:
    for attr in group_variable_attrs_min_aggregate:
      group_attr_key = "group[" + group_attr + ']["'  + attr + '"]'
      min += ((" " * global_indentation) + group_attr_key + " = " + attr.split(".")[1] + "\n")
  global_indentation -= 2 
  min += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  min += ((" " * global_indentation) + "if " + func[1] + " < " + group_key + ":\n")
  global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = " + func[1] + "\n")
  if group_variable_attrs_min_aggregate:
    for attr in group_variable_attrs_min_aggregate:
      group_attr_key = "group[" + group_attr + ']["'  + attr + '"]'
      min += ((" " * global_indentation) + group_attr_key + " = " + attr.split(".")[1] + "\n")

  return min


def countScript(group_attr, func, global_indentation, condition):
  """write the script of count aggregation function"""
  
  count = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  
  if condition:
    count += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  count += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  count += ((" " * global_indentation) + group_key + " = 1\n")
  global_indentation -= 2 
  count += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  count += ((" " * global_indentation) + group_key + " += 1\n\n")

  return count

def sumScript(group_attr, func, global_indentation, condition):
  """write the script of sum aggregation function"""
  
  sum = ""

  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  
  if condition:
    sum += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  sum += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  sum += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  global_indentation -= 2 
  sum += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  sum += ((" " * global_indentation) + group_key + " += " + func[1]  + "\n\n")

  return sum

def noAggregate(group_attr, attrs, global_indentation, condition):
  """write the script without aggregation function"""
  
  update = ""

  update += ((" " * global_indentation) + "if " + condition + ":\n")
  global_indentation += 2

  for attr in attrs:
    group_key = "group[" + group_attr + ']["'  + attr + '"]'
    update += ((" " * global_indentation) + group_key + " = " + attr.split(".")[1]  + "\n")


  return update