def checkOperands(input_file):
  # use a dict to store all operands' values
  operands = {
    "SELECT ATTRIBUTE(S)" : [],
    "NUMBER OF GROUPING VARIABLES(n)": [],
    "GROUPING ATTRIBUTES(V)": [],
    "F-VECT([F])": [],
    "SELECT CONDITION-VECT([σ])": [],
    "HAVING_CONDITION(G)": []
  }

  # split input with new line
  chunks = input_file.split('\n')
  operand = ""

  for chunk in chunks:
    # if the chunk is an operand, record it's name then read next line
    if chunk[:-1] in operands:
      operand = chunk[:-1]
      continue

    # if not an operand, it's a value, split it then put into operands dict
    spliited = chunk.split(", ")
    for value in spliited:
      operands[operand].append(value)

  return operands
