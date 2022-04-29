def convertMFStructure(input_file):
  """convert the S and F lists to mf_structure hashtable"""

  mf_structure = {}

  for attr in input_file["SELECT ATTRIBUTE(S)"]:
    mf_structure[attr] = None
  
  for attr in input_file["F-VECT([F])"]:
    mf_structure[attr] = None
  
  return mf_structure

  
  

