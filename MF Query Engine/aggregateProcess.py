def avgScript(group_attr, func, global_indentation):
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
  avg += ((" " * global_indentation) + count_key + " += 1\n")
  avg += ((" " * global_indentation) + group_key + " += ((" + func[1] + " - " + group_key + ")/" + count_key + ")\n")
  
  return avg