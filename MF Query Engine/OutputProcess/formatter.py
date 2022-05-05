def formatterScript(global_indentation):
  """The function to write the formatter process script"""

  type_formatter = ("\n" + (" " * global_indentation) + 'row_formatter = []\n')
  type_formatter += ((" " * global_indentation) + 'title_formatter = []\n')
  type_formatter += ((" " * global_indentation) + "for i, t in enumerate(columns_type):\n")
  global_indentation += 2
  type_formatter += ((" " * global_indentation) + 'if t == "str" or t == "dt":\n')
  global_indentation += 2
  type_formatter += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":<15}")\n')
  type_formatter += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  type_formatter += ((" " * global_indentation) + 'elif t == "float":\n')
  global_indentation += 2
  type_formatter += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":>15,.2f}")\n')
  type_formatter += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  type_formatter += ((" " * global_indentation) + "else:\n")
  global_indentation += 2
  type_formatter += ((" " * global_indentation) + 'row_formatter.append("{col" +str(i + 1) + ":>15}")\n')
  type_formatter += ((" " * global_indentation) + 'title_formatter.append("{:<15}")\n')
  global_indentation -= 2
  global_indentation -= 2

  type_formatter += ((" " * global_indentation) + 'title_formatter = "|".join(title_formatter)\n')
  type_formatter += ((" " * global_indentation) + 'row_formatter = "|".join(row_formatter)\n')
  return type_formatter