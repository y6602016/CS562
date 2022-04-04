def avgScript(group_attr, func, global_indentation, condition):
  avg = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  count_key = "count_" + func[1] + "[" + group_attr + "]"

  avg += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  avg += ((" " * global_indentation) + group_key + " = " + func[1] + "\n")
  avg += ((" " * global_indentation) + count_key + " += 1\n")
  global_indentation -= 2 

  avg += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  if condition:
    avg += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  avg += ((" " * global_indentation) + count_key + " += 1\n")
  avg += ((" " * global_indentation) + group_key + " += ((" + func[1] + " - " + group_key + ")/" + count_key + ")\n")
  
  return avg


def maxScript(group_attr, func, global_indentation, condition):
  max = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'

  max += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = " + func[1] + "\n")
  global_indentation -= 2 
  max += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  if condition:
    avg += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  max += ((" " * global_indentation) + group_key + " = max(" + func[1] + ", " + group_key + ")\n\n")

  return max


def minScript(group_attr, func, global_indentation, condition):
  min = ""
  group_key = "group[" + group_attr + ']["'  + func[2] + '"]'
  
  min += ((" " * global_indentation) + "if not " + group_key + ":\n")
  global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = " + func[1] + "\n")
  global_indentation -= 2 
  min += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  if condition:
    avg += ((" " * global_indentation) + "if " + condition + ":\n")
    global_indentation += 2
  min += ((" " * global_indentation) + group_key + " = min(" + func[1] + ", " + group_key + ")\n\n")

  return min