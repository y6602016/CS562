def convertMFStructure(input_file, cursor):
  # 
  query = "select column_name, data_type from information_schema.columns where table_name = 'sales'"

  cursor.execute(query)

  data_type = {}
  for row in cursor:
    data_type[row[0]] = row[1]
  
  
  


  
  
# if __name__ == '__main__':
#   convertMFStructure()
