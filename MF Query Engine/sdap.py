import psycopg2
import collections
import sys
from config import config

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	      # # execute a statement
        # print('PostgreSQL database version:')
        # cur.execute('SELECT version()')

        # # display the PostgreSQL database server version
        # db_version = cur.fetchone()
        # print(db_version)

        print("Pivoting")
        Pivoting(cur)

        print("=============")

        print("Dependent Aggregation")
        DependentAggregation(cur)

	      # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            # print('Database connection closed.')

def Pivoting(cursor):
  # strategy:
  # first scan: scan the results and record the quant and numbers in seperate tables
  # finally calculate the avg of each cust for three states

  # create a hash table where key = cust name, value = list [AVG_NY, AVG_CT, AVG_NJ]
  total_table = collections.defaultdict(lambda: [0, 0, 0])
  num_of_transaction_table = collections.defaultdict(lambda: [0, 0, 0])

  # define the query
  query = "select * from sales;"

  # execute the query
  cursor.execute(query)

  # first scan the result
  for row in cursor:
    # extract the row's name, state and quant
    name, state, quant = row[0], row[5], row[6]

    # based on the state, update two tables
    if state == "NY":
      total_table[name][0] += quant
      num_of_transaction_table[name][0] += 1
    elif state == "CT":
      total_table[name][1] += quant
      num_of_transaction_table[name][1] += 1
    elif state == "NJ":
      total_table[name][2] += quant
      num_of_transaction_table[name][2] += 1
  
  
  # calculate the avg and display the result with format
  title = "{:<10} {:>10} {:>10} {:>10}"
  print(title.format("CUST", "AVG_NY", "AVG_CT", "AVG_NJ"))
  print(title.format("======", "=========", "=========", "========="))
  s = "{:<10} {:>10,.2f} {:>10,.2f} {:>10,.2f}"
  for name, states in total_table.items():
    avg_ny = states[0] / num_of_transaction_table[name][0]
    avg_ct = states[1] / num_of_transaction_table[name][1]
    avg_nj = states[2] / num_of_transaction_table[name][2]
    print(s.format(name, avg_ny, avg_ct, avg_nj))


def DependentAggregation(cursor):
  # strategy: 
  # 1. first scan, calculate each (prod, month) average quant first
  # 2. second scan, count the transactions of each (prod, month) that is 25% greater than adjancent months

  prod_month_quant = collections.defaultdict(int)
  prod_month_transactions = collections.defaultdict(int)

  # define the query
  query = "select * from sales where year=2017;"
  
  # execute the query
  cursor.execute(query)
  
  # 1. first scan
  for row in cursor:
    prod, month, quant = row[1], row[3], row[6]
    prod_month_quant[(prod, month)] += quant
    prod_month_transactions[(prod, month)] += 1

  prod_month_avg = collections.defaultdict(float)
  
  for combination, quant in prod_month_quant.items():
    prod_month_avg[combination] = prod_month_quant[combination] / prod_month_transactions[combination]
  
  # used for record the valid counts
  prod_month_count = collections.defaultdict(int)

  # execute the query again for the second scan
  cursor.execute(query)

  # 2. second scan
  for row in cursor:
    prod, month, quant = row[1], row[3], row[6]
    if month == 1:
      if quant > prod_month_avg[(prod, month + 1)] * 1.25:
        prod_month_count[(prod, month)] += 1
    elif month == 12:
      if quant > prod_month_avg[(prod, month - 1)] * 1.25:
        prod_month_count[(prod, month)] += 1
    else:
      if quant > prod_month_avg[(prod, month - 1)] * 1.25 or quant > prod_month_avg[(prod, month + 1)] * 1.25:
        prod_month_count[(prod, month)] += 1

  # calculate the avg and display the result with format
  title = "{:<10} {:>10} {:>10}"
  print(title.format("PRODUCT", "MONTH", "COUNT"))
  print(title.format("======", "======", "=========="))
  s = "{:<10} {:>10} {:>10}"
  for (prod, month), count in prod_month_count.items():
    count = None if count == 0 else count
    print(s.format(prod, month, count))

if __name__ == '__main__':
    connect()