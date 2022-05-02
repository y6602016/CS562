# MF/EMF Query Process Engine
The query process engine for Ad-Hoc OLAP queries.<br />The query construct is based on an extended SQL syntax known as MF and EMF queries
<br /><br />
## Program Features
Ad-hoc OLAP queries (also known as multi-dimensional queries) expressed in standard SQL, even the simplest types, often lead to complex relational algebraic expressions with multiple joins, group-bys, and sub-queries. When faced with the challenges of processing such queries, traditional query optimizers do not consider the “big picture”. Rather, they try to optimize a series of joins and group-bys, leading to poor performance. <br />The program provides a syntactic framework to allow succinct expression of ad-hoc OLAP queries by extending the group-by statement and adding the new clause, such that, and in turn, provide a simple, efficient and scalable algorithm to process the queries.<br /> Please refer to the following two research articles for further details on the new syntax and the corresponding processing algorithm:
<br />* “Querying Multiple Features of Groups in Relational Databases”, D. Chatziantoniou and K. Ross
<br />* “Evaluation of Ad Hoc OLAP: In-Place Computation”, D. Chatziantoniou
<br /><br />
### Dependencies
- Python3
- psycopg2
- Postgres
- Input file - query_input.txt<br />
  
The input for the query processing engine is the list of arguments for the new operator Φ (in place of the actual query represented in SQL). We assume the query has already been transformed into a corresponding relational algebraic expression. For example for the following query,
```
select cust, sum(x.quant), sum(y.quant), sum(z.quant) from sales
group by cust: x, y, z
such that s.cust = cust and x.state = ‘NY’
and s.cust = cust and y.state = ‘NJ’
and s.cust = cust and z.state = ‘CT’
having sum(x.quant) > 2 * sum(y.quant) or avg(x.quant) > avg(z.quant);
```
The above example input can be expressed as
```
SELECT ATTRIBUTE(S):
cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
NUMBER OF GROUPING VARIABLES(n):
3
GROUPING ATTRIBUTES(V):
cust
F-VECT([F]):
1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
SELECT CONDITION-VECT([σ]):
1.cust == cust and 1.state == ’NY’, 2.cust == cust and 2.state == ’NJ’, 3.cust == cust and 3.state == ’CT’ HAVING_CONDITION(G):
1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant
```
There are some requirements of the input parameters. We assume:
<br />
1. SELECT ATTRIBUTE(S):
- All grouping attributes are included

2. SELECT CONDITION-VECT([σ]):
- Space between operators
- Started with the definition of the grouping attributes
- All conditions are in the same line, no new line
- Order is same as the order of given grouping variables
<br />

Some test inputs are provided in the "Test cases" folders. Please copy the part above "Standard SQL" and paste to query_input.txt. The query_input.txt file is the only file for input reading.
<br /><br />
### Executing program
- Activate Postgres Database server
- Activate virtual environment
- Install the packages
  ```
  $ pip install -r requirements.txt
  ```
- Execute the program
  ```
  python3 mfqe.py
  ```
- Select 1 for reading the input file or Select 2 for providing input from keyboard. 
- Execute the generated query program
  ```
  python3 query.py
  ```
