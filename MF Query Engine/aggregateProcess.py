def avgScript(group_attr, func, group_variable, global_indentation, condition):
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


def maxScript(group_attr, func, group_variable, global_indentation, condition):
  max = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'

  max += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  global_indentation -= 2 
  max += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  if condition:
    max += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = max(" + func[1]  + ", " + group_key + ")\n\n")

  return max


def minScript(group_attr, func, group_variable, global_indentation, condition):
  min = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  
  min += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = " + func[1]  + "\n")
  global_indentation -= 2 
  min += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  if condition:
    min += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = min(" + func[1]  + ", " + group_key + ")\n\n")

  return min


def countScript(group_attr, func, group_variable, global_indentation, condition):
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

def sumScript(group_attr, func, group_variable, global_indentation, condition):
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