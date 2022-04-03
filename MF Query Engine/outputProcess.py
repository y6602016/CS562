def writeMFStructure(mf_structure, script, globalIndentation):
  mf_structure = str(mf_structure)[1:-1].split(", ")

  script += ((" " * 2 * globalIndentation) + "mf_structure = {\n")

  globalIndentation += 1

  for i, item in enumerate(mf_structure):
    if i != len(mf_structure) - 1:
      script += (("\t" * 2 * globalIndentation) + item + ",\n")
    else:
      script += (("\t" * 2 * globalIndentation) + item + "\n")

  globalIndentation -= 1
  script += (("\t" * 2 * globalIndentation) + "}\n")
  return script