## How to read the CSV file 
---------------------------------
df = spark.read.format("csv") \
    .option("Header", True) \
    .option("Inferschema", True) \
    .option("sep", ",") \
    .load("/FileStore/tables/snakes_count_100.csv")
display(df)

## Another way to read the CSV file 
-----------------------------------
df1 = spark.read.format("csv") \
      .options(header = "True", inferschema ="True", sep =",") \
      .load("dbfs:/FileStore/tables/snakes_count_10000.csv")
display(df1.count())

## Reading Multiple files []
----------------------------
dfx = spark.read.format("csv") \
      .options(header = "True", inferschema = "True", sep =",") \
      .load(["/FileStore/tables/snakes_count_100.csv","dbfs:/FileStore/tables/snakes_count_10000.csv"])
      
## Reading all the files under the folder
-----------------------------------------
dfc = spark.read.format("csv") \
      .options(header = "True", inferschema = "True", sep = ",") \
      .load("dbfs:/FileStore/tables")
display(dfc)
print(dfc.count())

dfc.printSchema() --> show the structure 

## How to Create Own schema in Pyspark
-----------------------------------------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_Field = StructType([StructField('id', IntegerType(), True),
                             StructField('name', StringType(), True),
                             StructField('nationality', StringType(), True),
                             StructField('club_team', StringType(), True),
                             StructField('wage_euro', IntegerType(), True)
                             ])
                             
dft = spark.read.format("csv").schema(schema_Field).option("header", True).load('dbfs:/FileStore/tables/')
display(dft)

## alternative way to create Own Schema
--------------------------------------
alternative_schema = ' id Integer, name string, nationality string, club_team string, wage_euro Int'

dff = spark.read.format("csv").schema(alternative_schema).option("header", True).load('dbfs:/FileStore/tables/')
display(dff)


## Filter Conditions:
--------------------
• filter(df.column != 50)
• filter((f.col'column1') > 50) & (f.col'column2') > 50))
• filter((f.col('column1*) > 50)|(f.col('column2') > 50))
• filter(df.column.isNull))
• filter(df.column.isNotNulI())
• filter(df.column.like('%%* ))
• filter(df.name.isin())
• filter(df.column.contains('' ))
• filter(df.column.startswith(''))
• filter(df.column.endswith(''))

##Creating a Dataframe 
-----------------------
employee_data =[(10, "Raj kumar", "1999", "100", "M", 2000),
                (11, "suresh", "1989", "200", "M", 2100),
                (12, "pavan", "1899", "1001", "M", 2700),
                (14, "rukesh", "1699", "101", "f", 2500),
                (15, "jeevan", "1979", "102", "M", 2500)]
employee_schema = ["employee_id", "name", "doj", "employee_dept_id", "gender", "salary" ]

employee_df =spark.createDataFrame(data=employee_data, schema = employee_schema)
display(employee_df)


## Adding column with values 
------------------------------
from pyspark.sql.functions import lit 

example1:
empdf = fg.withColumn("Location", lit("Mumbai")).show()


example2:
tiger = employee_df1.withColumn("Location", lit("Mumbai")).show()

+-----------+---------+----+----------------+------+------+--------+
|employee_id|     name| doj|employee_dept_id|gender|salary|Location|
+-----------+---------+----+----------------+------+------+--------+
|         10|Raj kumar|1999|             100|     M|  2000|  Mumbai|
|         11|   suresh|1989|             200|     M|  2100|  Mumbai|
|         12|    pavan|1899|            1001|     M|  2700|  Mumbai|
|         14|   rukesh|1699|             101|     f|  2500|  Mumbai|
|         15|   jeevan|1979|             102|     M|  2500|  Mumbai|
+-----------+---------+----+----------------+------+------+--------+

## Adding new column with calculation 
----------------------------------------
example1:
from pyspark.sql.functions import concat
Calculation = employee_df1.withColumn("Bonus", employee_df1.salary*0.1).show()

+-----------+---------+----+----------------+------+------+-----+
|employee_id|     name| doj|employee_dept_id|gender|salary|Bonus|
+-----------+---------+----+----------------+------+------+-----+
|         10|Raj kumar|1999|             100|     M|  2000|200.0|
|         11|   suresh|1989|             200|     M|  2100|210.0|
|         12|    pavan|1899|            1001|     M|  2700|270.0|
|         14|   rukesh|1699|             101|     f|  2500|250.0|
|         15|   jeevan|1979|             102|     M|  2500|250.0|
+-----------+---------+----+----------------+------+------+-----+

example2:
from pyspark.sql.functions import concat 
calculations1 = Calculation.withColumn("Base salary", Calculation.Bonus*(25%100)).show()

+-----------+---------+----+----------------+------+------+-----+-----------+
|employee_id|     name| doj|employee_dept_id|gender|salary|Bonus|Base salary|
+-----------+---------+----+----------------+------+------+-----+-----------+
|         10|Raj kumar|1999|             100|     M|  2000|200.0|     5000.0|
|         11|   suresh|1989|             200|     M|  2100|210.0|     5250.0|
|         12|    pavan|1899|            1001|     M|  2700|270.0|     6750.0|
|         14|   rukesh|1699|             101|     f|  2500|250.0|     6250.0|
|         15|   jeevan|1979|             102|     M|  2500|250.0|     6250.0|
+-----------+---------+----+----------------+------+------+-----+-----------+

### Adding two or more Columns at a time 
----------------------------------------
from pyspark.sql.functions import concat 
addingmorecolumns = calculations1.withColumn("example salary", lit(5000)).withColumn("Name_with_id", concat("name","employee_id")).show()

+-----------+---------+----+----------------+------+------+-----+-----------+--------------+------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|Bonus|Base salary|example salary|Name_with_id|
+-----------+---------+----+----------------+------+------+-----+-----------+--------------+------------+
|         10|Raj kumar|1999|             100|     M|  2000|200.0|     5000.0|          5000| Raj kumar10|
|         11|   suresh|1989|             200|     M|  2100|210.0|     5250.0|          5000|    suresh11|
|         12|    pavan|1899|            1001|     M|  2700|270.0|     6750.0|          5000|     pavan12|
|         14|   rukesh|1699|             101|     f|  2500|250.0|     6250.0|          5000|    rukesh14|
|         15|   jeevan|1979|             102|     M|  2500|250.0|     6250.0|          5000|    jeevan15|
+-----------+---------+----+----------------+------+------+-----+-----------+--------------+------------+

## adding space between the name and id 
----------------------------------------
addingmorecolumns = calculations1.withColumn("Name_with_id", lit("_"),"employee_id").show()

+-----------+---------+----+----------------+------+------+-----+-----------+------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|Bonus|Base salary|Name_with_id|
+-----------+---------+----+----------------+------+------+-----+-----------+------------+
|         10|Raj kumar|1999|             100|     M|  2000|200.0|     5000.0|Raj kumar_10|
|         11|   suresh|1989|             200|     M|  2100|210.0|     5250.0|   suresh_11|
|         12|    pavan|1899|            1001|     M|  2700|270.0|     6750.0|    pavan_12|
|         14|   rukesh|1699|             101|     f|  2500|250.0|     6250.0|   rukesh_14|
|         15|   jeevan|1979|             102|     M|  2500|250.0|     6250.0|   jeevan_15|
+-----------+---------+----+----------------+------+------+-----+-----------+------------+

## Renaming the Column Name:
-------------------------------
example1:
Rename = addingspace.withColumnRenamed("Name_with_id","Name_ID")
display(Rename)

+-----------+---------+----+----------------+------+------+-----+-----------+------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|Bonus|Base salary|     Name_ID|
+-----------+---------+----+----------------+------+------+-----+-----------+------------+
|         10|Raj kumar|1999|             100|     M|  2000|200.0|     5000.0|Raj kumar_10|
|         11|   suresh|1989|             200|     M|  2100|210.0|     5250.0|   suresh_11|
|         12|    pavan|1899|            1001|     M|  2700|270.0|     6750.0|    pavan_12|
|         14|   rukesh|1699|             101|     f|  2500|250.0|     6250.0|   rukesh_14|
|         15|   jeevan|1979|             102|     M|  2500|250.0|     6250.0|   jeevan_15|
+-----------+---------+----+----------------+------+------+-----+-----------+------------+

example2:
Rename = addingspace.withColumnRenamed("Name_with_id","Name_ID").withColumnRenamed("doj","Date_of_Joining").show()
+-----------+---------+---------------+----------------+------+------+-----+-----------+------------+
|employee_id|     name|Date_of_Joining|employee_dept_id|gender|salary|Bonus|Base salary|     Name_ID|
+-----------+---------+---------------+----------------+------+------+-----+-----------+------------+
|         10|Raj kumar|           1999|             100|     M|  2000|200.0|     5000.0|Raj kumar_10|
|         11|   suresh|           1989|             200|     M|  2100|210.0|     5250.0|   suresh_11|
|         12|    pavan|           1899|            1001|     M|  2700|270.0|     6750.0|    pavan_12|
|         14|   rukesh|           1699|             101|     f|  2500|250.0|     6250.0|   rukesh_14|
|         15|   jeevan|           1979|             102|     M|  2500|250.0|     6250.0|   jeevan_15|
+-----------+---------+---------------+----------------+------+------+-----+-----------+------------+

## Drop a Column 
----------------
drop = Rename.drop("Name_ID").drop("Date_of_Joining").show()

+-----------+---------+----------------+------+------+-----+-----------+
|employee_id|     name|employee_dept_id|gender|salary|Bonus|Base salary|
+-----------+---------+----------------+------+------+-----+-----------+
|         10|Raj kumar|             100|     M|  2000|200.0|     5000.0|
|         11|   suresh|             200|     M|  2100|210.0|     5250.0|
|         12|    pavan|            1001|     M|  2700|270.0|     6750.0|
|         14|   rukesh|             101|     f|  2500|250.0|     6250.0|
|         15|   jeevan|             102|     M|  2500|250.0|     6250.0|
+-----------+---------+----------------+------+------+-----+-----------+

## JOINS:
---------
## Inner Join
-------------
inner = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "inner" )
display(inner)

+-----------+---------+----+----------------+------+------+---------------+-------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|department_name|department_id|
+-----------+---------+----+----------------+------+------+---------------+-------------+
|         10|Raj kumar|1999|             100|     M|  2000|             HR|          100|
|         11|   suresh|1989|             200|     M|  2100|         Supply|          200|
+-----------+---------+----+----------------+------+------+---------------+-------------+



## Full-Outer Join --> Gives the result of common tables + left join tables + right join tables 
------------------------------------------------------------------------------------------------
Outer_join = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "full")
display(Outer_join)

+-----------+---------+----+----------------+------+------+---------------+-------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|department_name|department_id|
+-----------+---------+----+----------------+------+------+---------------+-------------+
|         10|Raj kumar|1999|             100|     M|  2000|             HR|          100|
|         14|   rukesh|1699|             101|     f|  2500|           null|         null|
|         15|   jeevan|1979|             102|     M|  2500|           null|         null|
|         11|   suresh|1989|             200|     M|  2100|         Supply|          200|
|       null|     null|null|            null|  null|  null|          Sales|          300|
|       null|     null|null|            null|  null|  null|          Stock|          400|
|         12|    pavan|1899|            1001|     M|  2700|           null|         null|

## Left outer join 
---------------------
Outer_join = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "Left").show()

+-----------+---------+----+----------------+------+------+---------------+-------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|department_name|department_id|
+-----------+---------+----+----------------+------+------+---------------+-------------+
|         10|Raj kumar|1999|             100|     M|  2000|             HR|          100|
|         11|   suresh|1989|             200|     M|  2100|         Supply|          200|
|         12|    pavan|1899|            1001|     M|  2700|           null|         null|
|         14|   rukesh|1699|             101|     f|  2500|           null|         null|
|         15|   jeevan|1979|             102|     M|  2500|           null|         null|
+-----------+---------+----+----------------+------+------+---------------+-------------+

## Right outer join 
--------------------
Outer_join = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "Right").show()

+-----------+---------+----+----------------+------+------+---------------+-------------+
|employee_id|     name| doj|employee_dept_id|gender|salary|department_name|department_id|
+-----------+---------+----+----------------+------+------+---------------+-------------+
|         10|Raj kumar|1999|             100|     M|  2000|             HR|          100|
|         11|   suresh|1989|             200|     M|  2100|         Supply|          200|
|       null|     null|null|            null|  null|  null|          Sales|          300|
|       null|     null|null|            null|  null|  null|          Stock|          400|
+-----------+---------+----+----------------+------+------+---------------+-------------+


## Left Semi join --> similar to inner join, eliminates the right results 
-------------------------------------------------------------------------
Left_semi_join = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "leftsemi").show()

+-----------+---------+----+----------------+------+------+
|employee_id|     name| doj|employee_dept_id|gender|salary|
+-----------+---------+----+----------------+------+------+
|         10|Raj kumar|1999|             100|     M|  2000|
|         11|   suresh|1989|             200|     M|  2100|
+-----------+---------+----+----------------+------+------+


## Left anti join --> eliminates the commom records and gives the o/p
---------------------------------------------------------------------- 
Left_semi_join = employee_dfx.join(department_details, employee_dfx.employee_dept_id == department_details.department_id, "anti").show()

+-----------+------+----+----------------+------+------+
|employee_id|  name| doj|employee_dept_id|gender|salary|
+-----------+------+----+----------------+------+------+
|         12| pavan|1899|            1001|     M|  2700|
|         14|rukesh|1699|             101|     f|  2500|
|         15|jeevan|1979|             102|     M|  2500|
+-----------+------+----+----------------+------+------+

## DBUTILS :
--------
dbutils.fs.help()
dbutils.notebook.help()
dbutils.widgets.help()
----------------------
dbutils.widgets provides utilities for working with notebook widgets. You can create different types of widgets and get their bound value. For more info about a method, use dbutils.widgets.help("methodName").
       
combobox(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a combobox input widget with a given name, default value and choices
      
dropdown(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a dropdown input widget a with given name, default value and choices

get(name: String): String -> Retrieves current value of an input widget

getArgument(name: String, optional: String): String -> (DEPRECATED) Equivalent to get

multiselect(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a multiselect input widget with a given name, default value and choices

remove(name: String): void -> Removes an input widget from the notebook

removeAll: void -> Removes all widgets in the notebook

text(name: String, defaultValue: String, label: String): void -> Creates a text input widget with a given name and default value

dbutils.secrets.help("get")
dbutils.fs.help("cp")
dbutils.notebook.help("exit")


#DBUTILS COMMANDS 
------------------
##list filesystem
-----------------
dbutils.fs.ls("dbfs:/FileStore")
        Out[10]: [FileInfo(path='dbfs:/FileStore/shared_uploads/', name='shared_uploads/', size=0, modificationTime=0),
        FileInfo(path='dbfs:/FileStore/tables/', name='tables/', size=0, modificationTime=0),
        FileInfo(path='dbfs:/FileStore/tables_output/', name='tables_output/', size=0, modificationTime=0
##Head 
-------
dbutils.fs.head("/FileStore/tables/snakes_count_100.csv")
        Out[11]: '"Game Number", "Game Length"\n1, 30\n2, 29\n3, 31\n4, 16\n5, 24\n6, 29\n7, 28\n8, 117\n9, 42\n10, 23\n11, 40\n12, 15\n13, 18\n14, 51\n15, 15\n16, 19\n17, 30\n18, 25\n19, 17\n20, 55\n21, 20\n22, 12\n23, 39\n24, 25\n25, 56\n26, 61\n27, 77\n28, 34\n29, 14\n30, 8\n31, 31\n32, 34\n33, 22\n34, 12\n35, 52\n36, 50\n37, 24\n38, 20\n39, 91\n40, 33\n41, 27\n42, 25\n43, 83\n44, 12\n45, 21\n46, 38\n47, 20\n48, 24\n49, 37\n50, 18\n51, 56\n52, 20\n53, 47\n54, 51\n55, 22\n56, 46\n57, 24\n58, 35\n59, 28\n60, 12\n61, 28\n62, 43\n63, 50\n64, 37\n65, 127\n66, 11\n67, 12\n68, 22\n69, 25\n70, 57\n71, 53\n72, 38\n73, 33\n74, 16\n75, 35\n76, 29\n77, 23\n78, 21\n79, 11\n80, 31\n81, 12\n82, 37\n83, 40\n84, 29\n85, 14\n86, 15\n87, 38\n88, 74\n89, 19\n90, 121\n91, 52\n92, 25\n93, 14\n94, 12\n95, 31\n96, 25\n97, 16\n98, 19\n99, 54\n100, 24\n'

##Creating new folder 
---------------------
dbutils.fs.mkdirs("dbfs:/FileStore/tables/filesystem")

##Copy file to folder 
---------------------
dbutils.fs.cp("/FileStore/tables/snakes_count_100.csv","dbfs:/FileStore/tables/filesystem")

##move command
--------------
dbutils.fs.mv("/FileStore/tables/snakes_count_1000.csv","dbfs:/FileStore/tables/filesystem")

## Create a new file with content 
---------------------------------
dbutils.fs.put("/FileStore/tables/happy.txt", "happy days in future")

## how to see content of a file 
----------------------------
dbutils.fs.head("/FileStore/tables/happy.txt")

## remove a file 
----------------
dbutils.fs.rm("/FileStore/tables/happy.txt")

===============================
## DBUTILS NOTEBOOK COMMANDS |
===============================
## how to run another notebook --> works only in databricks full subscription
-----------------------------
dbutils.notebook.run("another notebook name", timeout in seconds)

## Creating dropdown box 
-------------------------
dbutils.widgets.dropdown("Dropdown","1", [str(x) for x in range(1, 11)])

## Creating combo box  --> there is no any difference between combo and dropdown box, we can only select one option
---------------------
dbutils.widgets.combobox("combobox","1", [str(x) for x in range(1, 11)])

##Multiselect option in widgets 
------------------------------
dbutils.widgets.multiselect("multiselect","car",("car","maruti","benz","volvo"))

#removing the widgets
----------------------
dbutils.widgets.remove("combobox")
dbutils.widgets.removeAll() --> removes all the widgets 



# Pyspark Explode Functions
---------------------------
# Create dataframe with array column 
------------------------------------
array_appliance = [
                   ('Raja',['Tv', 'Refrigirator','Oven', 'AC']),
                   ('Homesh', ['AC','Washingmachine',None]),
                   ('Ram',['Grinder','TV']),
                   ('Ramesh',['Refrigerator','TV',None]),
                   ('Rajesh',None)
                   ]
df_app = spark.createDataFrame(data = array_appliance, schema = ['name','Applainces'])
df_app.printSchema()
display(df_app)

    root
 |-- name: string (nullable = true)
 |-- Applainces: array (nullable = true)
 |    |-- element: string (containsNull = true)
 
 
#Explode array field #Explode array field 
-----------------------------------------

from pyspark.sql.functions import explode

df2 = df_app.select(df_app.name, explode(df_app.Applainces))

df_app.printSchema()
display(df_app)

df2.printSchema()
display(df2)

#explode outer --> This even will consider the NULL values 
----------------------------------------------------------
from pyspark.sql.functions import explode_outer

display(df_app.select(df_app.name, explode_outer(df_app.Applainces)))

#positional explode ---> displays the index numbers of the values
------------------------------------------------------------------
from pyspark.sql.functions import posexplode

display(df_app.select(df_app.name, posexplode(df_app.Applainces)))


#Positional explode with NULL ---> when the values has null values then the index also shows as null
-------------------------------
from pyspark.sql.functions import posexplode_outer
display(df_app.select(df_app.name, posexplode_outer(df_app.Applainces)))


## CASE FUNCTIONS:
-------------------#Sample DatFrame  
data_student = [("Raja","Science",80,"p",90),
                ("Rakesh","Maths",90,"p",70),
                ("Rama","English",20,"F",80),
                ("Rajesh","Science",45,"F",75),
                ("Ramesh","Maths",30,"F",50),
                ("Raghav","Maths",None,"NA",70)]
schema = ["name","Subject","Mark","status", "attendence"]
df = spark.createDataFrame(data=data_student, schema = schema)
df.printSchema()
display(df)

# CASE CONDITION, WHEN.OTHERWISE
---------------------------------
# Method 1 by using When condition:
---------------------------------
from pyspark.sql.functions import when 
dft = df.withColumn("Status", when(df.Mark >= 50, "Pass"). when(df.Mark <50,"Fail").otherwise("Absentee")).show()

(3) Spark Jobs
+------+-------+----+--------+----------+
|  name|Subject|Mark|  Status|attendence|
+------+-------+----+--------+----------+
|  Raja|Science|  80|    Pass|        90|
|Rakesh|  Maths|  90|    Pass|        70|
|  Rama|English|  20|    Fail|        80|
|Rajesh|Science|  45|    Fail|        75|
|Ramesh|  Maths|  30|    Fail|        50|
|Raghav|  Maths|null|Absentee|        70|
+------+-------+----+--------+----------+
 
# Method 2 by using expr function:
-----------------------------------
from pyspark.sql.functions import expr 
tgrx = tgr.withColumn("new_status", expr("CASE WHEN Mark >= 50 THEN 'PASS'"+ "WHEN Mark < 50 THEN 'FAIL'" + "ELSE 'Absence' END")).show()

+----+-------+----+--------+----------+----------+
|name|Subject|Mark|  status|attendence|new_status|
+----+-------+----+--------+----------+----------+
|   R|Science|  80|    pass|        90|      PASS|
|  Ra|  Maths|  90|    pass|        70|      PASS|
| Ram|English|  20|    Fail|        80|      FAIL|
| Raj|Science|  45|    Fail|        75|      FAIL|
|Rame|  Maths|  30|    Fail|        50|      FAIL|
|Ragh|  Maths|null|Absentee|        70|   Absence|
+----+-------+----+--------+----------+----------+

## Multiple Conditons 
---------------------
from pyspark.sql.functions import when 
df4 = tgrx.withColumn("Grade", when((tgrx.Mark >= 80) & (tgrx.attendence >=80), "Distinction") \
                    .when((tgrx.Mark >=50) & (tgrx.attendence >=50), "Good") \
                    .otherwise("Average")).show()
                    
+----+-------+----+--------+----------+----------+-----------+
|name|Subject|Mark|  status|attendence|new_status|      Grade|
+----+-------+----+--------+----------+----------+-----------+
|   R|Science|  80|    pass|        90|      PASS|Distinction|
|  Ra|  Maths|  90|    pass|        70|      PASS|       Good|
| Ram|English|  20|    Fail|        80|      FAIL|    Average|
| Raj|Science|  45|    Fail|        75|      FAIL|    Average|
|Rame|  Maths|  30|    Fail|        50|      FAIL|    Average|
|Ragh|  Maths|null|Absentee|        70|   Absence|    Average|
+----+-------+----+--------+----------+----------+-----------+

## How to find the SPARK VERSION 
---------------------------------
from pyspark.sql import SparkSession 

spark = SparkSession.builder.master("local").getOrCreate()
display(spark.sparkContext.version)

## UNION & UNIONALL
-------------------
## UNION fUNCTIONS:

example--
# Create DataFrame:
employee_data =[(100,"Stephen","1999","100","M",2000),
                 (200,"Philips","2002","200","M",8000),
                 (300,"John","2010","100","",6000)
               ]
employee_schema = ["employee_id","name","doj","employee_dept_id","gender","salary"]
df1 = spark.createDataFrame(data=employee_data, schema = employee_schema)
display(df1)

+-----------+-------+----+----------------+------+------+
|employee_id|   name| doj|employee_dept_id|gender|salary|
+-----------+-------+----+----------------+------+------+
|        100|Stephen|1999|             100|     M|  2000|
|        200|Philips|2002|             200|     M|  8000|
|        300|   John|2010|             100|      |  6000|
+-----------+-------+----+----------------+------+------+

# Create another DataFrame:
employee_data1 =[(300,"John","2010","100","",6000),
                 (400,"Nancy","2001","400","F",1000),
                 (500,"Rosy","2014","500","M",5000)
               ]
employee_schema1 = ["employee_id","name","doj","employee_dept_id","gender","salary"]
df2 = spark.createDataFrame(data=employee_data1, schema = employee_schema1)
display(df2)

+-----------+-----+----+----------------+------+------+
|employee_id| name| doj|employee_dept_id|gender|salary|
+-----------+-----+----+----------------+------+------+
|        300| John|2010|             100|      |  6000|
|        400|Nancy|2001|             400|     F|  1000|
|        500| Rosy|2014|             500|     M|  5000|
+-----------+-----+----+----------------+------+------+

#Union Function
---------------
union = df1.union(df2)
display(union)

+-----------+-------+----+----------------+------+------+
|employee_id|   name| doj|employee_dept_id|gender|salary|
+-----------+-------+----+----------------+------+------+
|        100|Stephen|1999|             100|     M|  2000|
|        200|Philips|2002|             200|     M|  8000|
|        300|   John|2010|             100|      |  6000|
|        300|   John|2010|             100|      |  6000|
|        400|  Nancy|2001|             400|     F|  1000|
|        500|   Rosy|2014|             500|     M|  5000|
+-----------+-------+----+----------------+------+------+

# How to drop the Duplicate values in the union function: dropDuplicates()
--------------------------------------------------------------------------
union = df1.union(df2)
display(union.dropDuplicates())


## Pivot and Un-pivot functions
-------------------------------
# PIVOT FUNCTION EXAMPLE
------------------------
data = [("ABC","Q1",2000),
        ("ABC","Q2",3000),
        ("ABC","Q3",6000),
        ("ABC","Q4",1000),
        ("XYZ","Q1",5000),
        ("XYZ","Q2",4000),
        ("XYZ","Q3",1000),
        ("XYZ","Q4",2000),
        ("KLM","Q1",2000),
        ("KLM","Q2",3000),
        ("KLM","Q3",1000),
        ("KLM","Q4",5000)
         ]
column = ["Company","Qauter","Revenue"]
DF = spark.createDataFrame(data = data, schema = column).show()
+-------+------+-------+
|Company|Qauter|Revenue|
+-------+------+-------+
|    ABC|    Q1|   2000|
|    ABC|    Q2|   3000|
|    ABC|    Q3|   6000|
|    ABC|    Q4|   1000|
|    XYZ|    Q1|   5000|
|    XYZ|    Q2|   4000|
|    XYZ|    Q3|   1000|
|    XYZ|    Q4|   2000|
|    KLM|    Q1|   2000|
|    KLM|    Q2|   3000|
|    KLM|    Q3|   1000|
|    KLM|    Q4|   5000|
+-------+------+-------+

#PIVOT FUNCTION :
pivot = DF.groupBy("Company").pivot("Qauter").sum("Revenue")
display(pivot)

+-------+----+----+----+----+
|Company|  Q1|  Q2|  Q3|  Q4|
+-------+----+----+----+----+
|    KLM|2000|3000|1000|5000|
|    XYZ|5000|4000|1000|2000|
|    ABC|2000|3000|6000|1000|
+-------+----+----+----+----+

#UN-PIVOT FUNCTION :
un_pivot = pivot.selectExpr("Company", "stack(4,'Q1',Q1,'Q2',Q2,'Q3',Q3,'Q4',Q4) as (Quater,Revenue)")
display(un_pivot)
+-------+------+-------+
|Company|Qauter|Revenue|
+-------+------+-------+
|    KLM|    Q1|   2000|
|    KLM|    Q2|   3000|
|    KLM|    Q3|   1000|
|    KLM|    Q4|   5000|
|    XYZ|    Q1|   5000|
|    XYZ|    Q2|   4000|
|    XYZ|    Q3|   1000|
|    XYZ|    Q4|   2000|
|    ABC|    Q1|   2000|
|    ABC|    Q2|   3000|
|    ABC|    Q3|   6000|
|    ABC|    Q4|   1000|
+-------+------+-------+

# HANDLING CORRUPTED RECORD 
1. We need to create the schema first
    -> we have 3 types to handle the corruption  
         1. Perissive mode 
         2. DropMalformed Mode 
         3. FailFast mode 
         
Permissive mode: will keep the records and shows the corrupt records 
        ->df = spark.read.format("csv).option("mode", "PERMISSIVE").option("header", True).schema(schema_name).load("file location")
        -> display(df)
        
DropMalformed mode: this function will ignore the corrupted records 
         -> df = spark.read.format("csv").option("mode", "DROPMALFORMED").option("header", True).schema(schema_name).load("file_location")
         -> display(df)

FailFast Mode : this function will not execute in case any corrupt records are found 
        -> df = spark.read.format("csv").option("mode", "FAILFAST").option("header". True).schema(schema_name).load("file_location")
        
# To check the default Parallelism
----------------------------------
-> sc.defaultParallelism
     -> o/p 8 by default 
     
# How to check the partition Bytes by default 
---------------------------------------------
->spark.conf.get("spark.sql.files.maxPartitionBytes")
      -> by default 128 MB
   
#Getting data with in the spark environment 
-------------------------------------------
 from pyspark.sql.types import IntegerType 
 df = spark.createDataFrame(range(10), IntegerType())
 df.rdd.getNumPartitions()
      -> Out[18]: 8
      
#HOW TO KNOW THE EXECUTORS HAS BEEN PARTITIONS BY DEFAULT VALUE 
----------------------------------------------------------------
df.rdd.glom().collect()

Out[21]: [[Row(value=0)],
 [Row(value=1)],
 [Row(value=2)],
 [Row(value=3), Row(value=4)],
 [Row(value=5)],
 [Row(value=6)],
 [Row(value=7)],
 [Row(value=8), Row(value=9)]]
 
#HOW TO PARTITION A LARGE FILE WITH EXAMPLE
-------------------------------------------
df = spark.read.format("csv").option("Header", True).option("Inferschema", True).load("dbfs:/FileStore/tables/fifa_cleaned.csv")
#df.rdd.getNumPartitions()
#df.rdd.glom().collect()
#spark.conf.get("spark.sql.files.maxPartitionBytes")
#spark.conf.set("spark.sql.files.maxPartitionBytes", 100)

#REPARTITION:
-------------
repartition is used to increase or decreased the values of the parallelism, shuffle the data across the nodes, it produce the evenly distributed parititons.
-> df.rdd.getNumPartitions()
    -> By default the value is 8 
-> df.rdd.repartition(5)
    -> Out[7]: 5
    
# COALESCE:
------------
this function is only used to reduce the partitions, doesnt require shuffling across the data across nodes, would merge partitions with in executors, it produce unevenly distributed partitions.
 #How to get the partition number ?
 ----------------------------------
    -> df.rdd.getNumPartitions()
            -> By default the value is 8 
            
 #How to resize the partition number?
 ------------------------------------
    -> df.rdd.repartition(3)
        -> df.rdd.getNumPartitions()
             -> Now the value is 3 
    -> df.rdd.glom().collect()
    
    
    
#WHAT IS CACHE AND PERSIST ?
----------------------------
its an API provided by spark or spark function through which we can store the data in memory or disk. 
cache: always common across all the databases and also in spark, when we need to store the data in memory we use the cache option, cache always store the inmemory across the nodes.

Persist: is a programming mechanism which gives an option to store the data either in-memory or in disc across the nodes.

--> They are used as optimization techniques to save interim computation results of data frames or datasets.

#BROADCAST VARIABLE 
-------------------
EXAMPLE
Transaction = [
			   (100,'Cosmetic',150),
			   (200,'Apparel',250),
			   (300,'Shirt',400),
			   (400,'Trouser',500),
			   (500,'Socks',20),
			   (100,'Belt',70),
			   (200,'Cosmetics',250),
			   (300,'Shoe',400),
			   (400,'Socks',25),
			   (500,'Shorts',100)
			   ]
TransactionDF = spark.createDataFrame(data = Transaction, schema = ['Store_id','Item','Amount'])

Store = [ (100,'Store_London'),
		  (200,'Store_Paris'),
		  (300,'Store_UAE'),
		  (400,'Store_Oslo'),
		  (500,'Store_USA')
        ]
storeDF = spark.createDataFrame(data = Store, schema = ['Store_id','store_name'])


from pyspark.sql.functions import broadcast 
broadcast = TransactionDF.join(broadcast(storeDF), TransactionDF['Store_id'] == storeDF['Store_id'])
display(broadcast)

broadcast.explain(True)

#ADAPTIVE QUERY EXECUTION:
-------------------------
# How to enable the adapative query execution ? 
               -> spark.conf.set("spark.sql.adaptive.enabled", True)

#How to disable the adaptive query execution ? 
               -> spark.conf.set("spark.sql.adapative.enabled", True)
               
# HOW TO DISABLE THE COST BASED OPTIMIZER? 
------------------------------------------
    -> spark.conf.set("spark.sql.cbo.enabled", True) -> to on the CBO.
    -> spark.conf.set("spark.sql.cbo.enabled", False) -> to turn off the CBO.
    
#HANDLING THE NULL IN THE FILTERS:
-----------------------------------
1. isNull() : 
        -> The function isNull() returns all the rows where certain column on which we apply this function contain Null values. 
    
command : 
        df.filter (df.column_name, isNull())
      
example:
-------
data_student = [("Michael", "Science", 80, "P", 90),
                ("Nancy", "Mathematics", 90, "p", None), 
                ("David", "English", 20, "F", 80),
                ("'John", "Science", None, "F", None), 
                ("_lessy" ,None, 30, "F", 50),
                ("Martin", "Mathematics", None, None, 70)]
Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema = Schema).show()
#display (df)

+-------+-----------+----+------+----------+
|   name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     p|      null|
|  David|    English|  20|     F|        80|
|  'John|    Science|null|     F|      null|
| _lessy|       null|  30|     F|        50|
| Martin|Mathematics|null|  null|        70|
+-------+-----------+----+------+----------+

#command:
---------
null = df.filter(df.Mark.isNull()).show()

o/p:
+------+-----------+----+------+----------+
|  name|    Subject|Mark|Status|Attendance|
+------+-----------+----+------+----------+
| 'John|    Science|null|     F|      null|
|Martin|Mathematics|null|  null|        70|
+------+-----------+----+------+----------+

=================================================================================================================================       
2. isNotNull() : 
        -> This function is opposite to isNull(), creates new dataframe by eliminating all the rows where certain column on which we     apply this function contains Null values.
        
command: 
        df.filter (df.column_name, isNotNull()) 
        
Examples :
---------
data_student = [("Michael", "Science", 80, "P", 90),
                ("Nancy", "Mathematics", 90, "p", None), 
                ("David", "English", 20, "F", 80),
                ("'John", "Science", None, "F", None), 
                ("_lessy" ,None, 30, "F", 50),
                ("Martin", "Mathematics", None, None, 70)]
Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame(data = data_student, schema = Schema).show()
#display (df)

notnull = df.filter(df.Mark.isNotNull()).show()
# o/p :
+-------+-----------+----+------+----------+
|   name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     p|      null|
|  David|    English|  20|     F|        80|
| _lessy|       null|  30|     F|        50|
+-------+-----------+----+------+----------+

#COMBINING TWO COLUMNS WITH IS NOTNULL TO GET THE RESULTS:
----------------------------------------------------------
display = df.filter((df.Mark.isNotNull()) & (df.Attendence.isNotNull()))

O/p: 
--- 
+-------+-------+----+------+----------+
|   name|Subject|Mark|Status|Attendance|
+-------+-------+----+------+----------+
|Michael|Science|  80|     P|        90|
|  David|English|  20|     F|        80|
| _lessy|   null|  30|     F|        50|
+-------+-------+----+------+----------+

# COMBINING TWO COLUMNS WITH ISNOTNULL with OR Condition :
----------------------------------------------------------
Orcondition  = df.filter((df.Mark.isNotNull()) | (df.Attendance.isNotNull())).show()

o/p: 
----
+-------+-----------+----+------+----------+
|   name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     p|      null|
|  David|    English|  20|     F|        80|
| _lessy|       null|  30|     F|        50|
| Martin|Mathematics|null|  null|        70|
+-------+-----------+----+------+----------+

#DROP FUNCTION :
----------------
Drop function is used to drop the null values in the data, we have all, any, subset options in the Drop function, by default any is the default option 

any :  --. this will drop all the Null values.
    df.dropna("any")
all : --. this will drop the null values only all the values in the row are null.
    df.dropna("all")
subset : --. this is used to mention the columns, we can mention more number of columns in a subset.
    df.dropna(subset = ["Column_name","Column2_name"]
    

#FILL FUNCTION:
---------------
This function is used to fill the values in the Null, this condition will be applied only to the integer values not for the string values.

-> df.fillna(value =0)
+-------+-----------+----+------+----------+
|  'name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     P|         0|
|  David|    English|  20|     F|        80|
|   John|    Science|   0|     F|         0|
| Martin|Mathematics|   0|  null|        70|
|   null|       null|   0|  null|         0|
+-------+-----------+----+------+----------+

#HOW TO APPLY THE FILL FUNCTION TO THE STRING VALUES WHERE NULL IS FOUND?
------------------------------------------------------------------------
df.fillna(value = "NA") ---> this will replace the last row with NA only for the STRING values.

+-------+-----------+----+------+----------+
|  'name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     P|      null|
|  David|    English|  20|     F|        80|
|   John|    Science|null|     F|      null|
| Martin|Mathematics|null|    NA|        70|
|     NA|         NA|null|    NA|      null|
+-------+-----------+----+------+----------+

#HOW TO APPLY THE FILL FUNCTION USING SUBSET ?
----------------------------------------------
df.fillna(values = 0, subset = ["Mark", "Attendence"]

o/p :
+-------+-----------+----+------+----------+
|  'name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     P|         0|
|  David|    English|  20|     F|        80|
|   John|    Science|   0|     F|         0|
| Martin|Mathematics|   0|  null|        70|
|   null|       null|   0|  null|         0|
+-------+-----------+----+------+----------+

#HOW TO FILL THE VALUES TO EACH COLUMN MANUALLY ? 
-------------------------------------------------
df.fillna({"Mark" : 0, "Status": "NA", "name":"no_name})

o/p:
+-------+-----------+----+------+----------+
|  'name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Science|  80|     P|        90|
|  Nancy|Mathematics|  90|     P|      null|
|  David|    English|  20|     F|        80|
|   John|    Science|   0|     F|      null|
| Martin|Mathematics|   0|    na|        70|
|no_name|       null|   0|    na|      null|
+-------+-----------+----+------+----------+

# UDF(USER DEFINED FUNCTIONS):
------------------------------
EXAMPLE :
---------
employee_data = [ (10, "Michael Robinson", "1999-06-01", "100", 2000),
                  (20, "James Wood", "2003-03-01", "200", 8000), 
                  (30, "Chris Andrews", "2005-04-01", "100", 6000), 
                  (40, "Mark Bond", "2008-10-01", "100", 7000), 
                  (50, "Steve Watson", "1996-02-01", "400", 1000), 
                  (60, "Mathews Simon", "1998-11-01", "500", 5000), 
                  (70, "Peter Paul", "2011-04-01", "600", 5000) 
                ]
employee_schema = ["employee_id", "Name", "doj", "employee_dept_id", "salary"]
empDF = spark. createDataFrame (data=employee_data, schema = employee_schema)
display (empDF)

FUNCTION CREATION:
-----------------
import pyspark.sql.functions as f 
def rename_columns(rename_df):
    for column in rename_df.columns:
        new_column ="col_"+column
        rename_df = rename_df.withColumnRenamed(column, new_column)
    return rename_df
    
renamed_df = rename_columns(empDF)
display(renamed_df)

+---------------+----------------+----------+--------------------+----------+
|col_employee_id|        col_Name|   col_doj|col_employee_dept_id|col_salary|
+---------------+----------------+----------+--------------------+----------+
|             10|Michael Robinson|1999-06-01|                 100|      2000|
|             20|      James Wood|2003-03-01|                 200|      8000|
|             30|   Chris Andrews|2005-04-01|                 100|      6000|
|             40|       Mark Bond|2008-10-01|                 100|      7000|
|             50|    Steve Watson|1996-02-01|                 400|      1000|
|             60|   Mathews Simon|1998-11-01|                 500|      5000|
|             70|      Peter Paul|2011-04-01|                 600|      5000|
+---------------+----------------+----------+--------------------+----------+

EXAMPLE2 : UPPER CASE FUNCTION 
------------------------------
from pyspark.sql.functions import upper,col 
def upperCase_col(df):
    empDF_upper = df.withColumn('name_upper', upper(df.Name))
    return empDF_upper
    
Up_Case_DF = upperCase_col(empDF)
display(Up_Case_DF)

+-----------+----------------+----------+----------------+------+----------------+
|employee_id|            Name|       doj|employee_dept_id|salary|      name_upper|
+-----------+----------------+----------+----------------+------+----------------+
|         10|Michael Robinson|1999-06-01|             100|  2000|MICHAEL ROBINSON|
|         20|      James Wood|2003-03-01|             200|  8000|      JAMES WOOD|
|         30|   Chris Andrews|2005-04-01|             100|  6000|   CHRIS ANDREWS|
|         40|       Mark Bond|2008-10-01|             100|  7000|       MARK BOND|
|         50|    Steve Watson|1996-02-01|             400|  1000|    STEVE WATSON|
|         60|   Mathews Simon|1998-11-01|             500|  5000|   MATHEWS SIMON|
|         70|      Peter Paul|2011-04-01|             600|  5000|      PETER PAUL|
+-----------+----------------+----------+----------------+------+----------------+

#COMPRESSION:
------------
SNAPPY:
--------
snappy_compression = df1.write.format("parquet").option("compression", "snappy").save("dbfs:/FileStore/tables/compression")

GZIP:
-----'
gzip_compression = df1.write.format("parquet").option("compression", "gzip").save(""dbfs:/FileStore/tables/compression/gzip")

#SPLIT FUNCTION
---------------
example for space 
-------------------
employee_data = [ (10, "Michael Robinson", "1999-06-01", "100", 2000),
                  (20, "James Wood", "2003-03-01", "200", 8000), 
                  (30, "Chris Andrews", "2005-04-01", "100", 6000), 
                  (40, "Mark Bond", "2008-10-01", "100", 7000), 
                  (50, "Steve Watson", "1996-02-01", "400", 1000), 
                  (60, "Mathews Simon", "1998-11-01", "500", 5000), 
                  (70, "Peter Paul", "2011-04-01", "600", 5000) 
                ]
employee_schema = ["employee_id", "Name", "doj", "employee_dept_id", "salary"]
employeedetails = spark. createDataFrame (data=employee_data, schema = employee_schema)
display (employeedetails)

from pyspark.sql.functions import split
split_function_for_space = employee.withColumn('First_Name', split(employeedetails['Name'], ' ').getItem(0) /
                                   .withColumn('Last_Name', split(employeedetails['Name'], ' ').getItem(1))
display(split_function_for_space)

+-----------+----------------+----------+----------------+------+---------+--------+
|employee_id|            Name|       doj|employee_dept_id|salary|FirstName|LastName|
+-----------+----------------+----------+----------------+------+---------+--------+
|         10|Michael Robinson|1999-06-01|             100|  2000|  Michael|Robinson|
|         20|      James Wood|2003-03-01|             200|  8000|    James|    Wood|
|         30|   Chris Andrews|2005-04-01|             100|  6000|    Chris| Andrews|
|         40|       Mark Bond|2008-10-01|             100|  7000|     Mark|    Bond|
|         50|    Steve Watson|1996-02-01|             400|  1000|    Steve|  Watson|
|         60|   Mathews Simon|1998-11-01|             500|  5000|  Mathews|   Simon|
|         70|      Peter Paul|2011-04-01|             600|  5000|    Peter|    Paul|
+-----------+----------------+----------+----------------+------+---------+--------+

example for Hyphen:
-------------------
employee_data = [ (10, "Michael Robinson", "1999-06-01", "100", 2000),
                  (20, "James Wood", "2003-03-01", "200", 8000), 
                  (30, "Chris Andrews", "2005-04-01", "100", 6000), 
                  (40, "Mark Bond", "2008-10-01", "100", 7000), 
                  (50, "Steve Watson", "1996-02-01", "400", 1000), 
                  (60, "Mathews Simon", "1998-11-01", "500", 5000), 
                  (70, "Peter Paul", "2011-04-01", "600", 5000) 
                ]
employee_schema = ["employee_id", "Name", "doj", "employee_dept_id", "salary"]
employeedetails = spark. createDataFrame (data=employee_data, schema = employee_schema)
display (employeedetails)

here we will be applying the split function to DOJ 

from pyspark.sql.functions import split
split_function_for_Hyphen = employeedetails.withColumn('Year', split(employeedetails['doj'], ' - ').getItem(0)) \
                                          .withColumn('Month', split(employeedetails['doj'], ' - ').getItem(1)) \
                                          .withColumn('Date', split(employeedetails['doj'], ' - ').getItem(2))
display(split_function_for_Hyphen)


#ARRAY_ZIP
-----------
EXAMPLE:
--------
array_data = [
                ("John", 4, 1), 
                ("John", 6, 2), 
                ("David", 7, 3), 
                ("Mike", 3, 4), 
                ("David", 5, 2), 
                ("John", 7, 3), 
                ("John", 9, 7),                         
                ("David", 1, 8), 
                ("David", 4, 9), 
                ("David", 7, 4), 
                ("Mike", 8, 5) , 
                ("Mike", 5, 2), 
                ("Mike", 3, 8), 
                ("John", 2, 7),
                ("David", 1, 9)
              ]
array_schema = ["Name","Score_1", "Score_2"]
arraydf = spark. createDataFrame(data=array_data, schema = array_schema)
display(arraydf)


+-----+-------+-------+
| Name|Score_1|Score_2|
+-----+-------+-------+
| John|      4|      1|
| John|      6|      2|
|David|      7|      3|
| Mike|      3|      4|
|David|      5|      2|
| John|      7|      3|
| John|      9|      7|
|David|      1|      8|
|David|      4|      9|
|David|      7|      4|
| Mike|      8|      5|
| Mike|      5|      2|
| Mike|      3|      8|
| John|      2|      7|
|David|      1|      9|
+-----+-------+-------+

we need to aggregrate the above data using the NAMES 

from pyspark.sql import functions as F 

aggregating = arraydf.groupby("Name").agg(F.collect_list("Score_1").alias("Array_Score_1"), F.collect_list("Score_2).alias("Array_Score_2))
display(aggregating)

+-----+------------------+------------------+
| Name|     Arrya_Score_1|           Score_2|
+-----+------------------+------------------+
| John|   [4, 6, 7, 9, 2]|   [1, 2, 3, 7, 7]|
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|
| Mike|      [3, 8, 5, 3]|      [4, 5, 2, 8]|
+-----+------------------+------------------+

Now we will combine the values column1 values with column2 values 


arrays_zip = aggregating.withColumn("Zipped_new_values", F.arrays_zip("Arrya_Score_1", "Score_2").show()

+-----+------------------+------------------+------------------------------------------------+
|Name |Arrya_Score_1     |Score_2           |New_zipped_values                               |
+-----+------------------+------------------+------------------------------------------------+
|John |[4, 6, 7, 9, 2]   |[1, 2, 3, 7, 7]   |[{4, 1}, {6, 2}, {7, 3}, {9, 7}, {2, 7}]        |
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|[{7, 3}, {5, 2}, {1, 8}, {4, 9}, {7, 4}, {1, 9}]|
|Mike |[3, 8, 5, 3]      |[4, 5, 2, 8]      |[{3, 4}, {8, 5}, {5, 2}, {3, 8}]                |
+-----+------------------+------------------+------------------------------------------------+

#HOW TO FLATTEN THE JSON FILE 
-------------------------------
Example:
--------
empDF = [('Sales_dept' ,[{'emp_name': 'John', 'salary': '1000', 'yrs_of_service': '10', 'Age': '33'},
                         {'emp_name': 'David', 'salary': '2000', 'yrs_of_service': '15', 'Age': '40'},
                         {'emp_name': 'Nancy', 'salary': '8000', 'yrs_of_service': '20', 'Age': '45'},
                         {'emp_name': 'Mike', 'salary': '3000', 'yrs_of_service': '6', 'Age': '30'}, 
                         {'emp_name': 'Rosy', 'salary': '6000', 'yrs_of_service': '8', 'Age': '32'}]),
        ('HR_dept', [{'emp_name': 'Edwin', 'salary': '6000', 'yrs_of_service': '8', 'Age': '31'},
                        {'emp_name': 'Tomas', 'salary': '3000', 'yrs_of_service': '4', 'Age': '26'},
                        {'emp_name': 'Sarah', 'salary': '12000', 'yrs_of_service': '22', 'Age': '49'}, 
                        {'emp_name': 'Stella', 'salary': '15000', 'yrs_of_service': '25', 'Age': '52'}, 
                        {'emp_name': 'Kevin', 'salary': '4000', 'yrs_of_service': '5', 'Age': '27'}])
]
df_b = spark.createDataFrame (data=empDF, schema = ['Department', 'Employee'])
df_b.printSchema ()
display (df_b)

next we need to use arrays_zip function 
---------------------------------------
from pyspark.sql import functions as F 

array = df_b.withColumn("ARRAY", F.arrays_zip(df_b['Employee']))
display(array)

after the arrays_zip function, we need to use the explode function 
------------------------------------------------------------------
explode = array.withColumn("explode", F.explode(array['ARRAY']))
display(explode)

again we need to use the explode function to extract the value and drop the previous values in the table
-------------------------------------------------------------------------------------------------------
explode_all = explode.withColumn("Employee_name", explode['explode.Employee.emp_name']) \
                     .withColumn("yrs_of_service", explode['explode.Employee.yrs_of_service']) \
                     .withColumn("salary", explode['explode.Employee.salary']) \
                     .withColumn("Age", explode['explode.Employee.Age']) \
                     .drop("explode","Employee","ARRAY")
display(explode_all)

after tranformtions we get the o/p as below 
--------------------------------------------
+----------+-------------+--------------+------+---+
|Department|Employee_name|yrs_of_service|salary|Age|
+----------+-------------+--------------+------+---+
|Sales_dept|         John|            10|  1000| 33|
|Sales_dept|        David|            15|  2000| 40|
|Sales_dept|        Nancy|            20|  8000| 45|
|Sales_dept|         Mike|             6|  3000| 30|
|Sales_dept|         Rosy|             8|  6000| 32|
|   HR_dept|        Edwin|             8|  6000| 31|
|   HR_dept|        Tomas|             4|  3000| 26|
|   HR_dept|        Sarah|            22| 12000| 49|
|   HR_dept|       Stella|            25| 15000| 52|
|   HR_dept|        Kevin|             5|  4000| 27|
+----------+-------------+--------------+------+---+


#ARRAY_INTERSECT
-----------------
empDF = [
        ('John', [4, 6, 7, 9, 21], [1, 2, 3, 7, 7]), 
        ('David', [7, 5, 1, 4, 7, 1], [3, 2, 8, 9, 4, 9]), 
        ('Mike', [3, 9, 1, 6, 2], [1, 2, 3, 5, 8])
        ]
df = spark. createDataFrame(data=empDF, schema = ['Name', 'Array_1', 'Array_2'])
df.show ()
+-----+------------------+------------------+
| Name|           Array_1|           Array_2|
+-----+------------------+------------------+
| John|  [4, 6, 7, 9, 21]|   [1, 2, 3, 7, 7]|
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|
| Mike|   [3, 9, 1, 6, 2]|   [1, 2, 3, 5, 8]|
+-----+------------------+------------------+

from pyspark.sql import functions as F
array_intersection = df.withColumn("joined", F.array_intersect("Array_1", "Array_2")).show()

+-----+------------------+------------------+---------+
| Name|           Array_1|           Array_2|   joined|
+-----+------------------+------------------+---------+
| John|  [4, 6, 7, 9, 21]|   [1, 2, 3, 7, 7]|      [7]|
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|      [4]|
| Mike|   [3, 9, 1, 6, 2]|   [1, 2, 3, 5, 8]|[3, 1, 2]|
+-----+------------------+------------------+---------+

#ARRAY_EXCEPT
-------------
empDF = [
        ('John', [4, 6, 7, 9, 21], [1, 2, 3, 7, 7]), 
        ('David', [7, 5, 1, 4, 7, 1], [3, 2, 8, 9, 4, 9]), 
        ('Mike', [3, 9, 1, 6, 2], [1, 2, 3, 5, 8])
        ]
df = spark. createDataFrame(data=empDF, schema = ['Name', 'Array_1', 'Array_2'])
df.show ()

+-----+------------------+------------------+
| Name|           Array_1|           Array_2|
+-----+------------------+------------------+
| John|  [4, 6, 7, 9, 21]|   [1, 2, 3, 7, 7]|
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|
| Mike|   [3, 9, 1, 6, 2]|   [1, 2, 3, 5, 8]|
+-----+------------------+------------------+

from pyspark.sql import functions as F 
array_except = df.withColumn('only_values', F.array_except('Array_1','Array_2')).show()

+-----+------------------+------------------+-------------+
| Name|           Array_1|           Array_2|  only_values|
+-----+------------------+------------------+-------------+
| John|  [4, 6, 7, 9, 21]|   [1, 2, 3, 7, 7]|[4, 6, 9, 21]|
|David|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|    [7, 5, 1]|
| Mike|   [3, 9, 1, 6, 2]|   [1, 2, 3, 5, 8]|       [9, 6]|
+-----+------------------+------------------+-------------+

#ARRAY_SORT
------------
empDF = [
        ('John', [4, 6, 7, 9, 21]), 
        ('David', [7, 5, 1, 4, 7, 1]), 
        ('Mike', [3, 9, 1, 6, 2])
        ]
df = spark. createDataFrame(data=empDF, schema = ['Name', 'Array_1'])
df.show ()
+-----+------------------+
| Name|           Array_1|
+-----+------------------+
| John|  [4, 6, 7, 9, 21]|
|David|[7, 5, 1, 4, 7, 1]|
| Mike|   [3, 9, 1, 6, 2]|
+-----+------------------+

from pyspark.sql import functions as F 
sort_array = df.withColumn("newSort", F.array_sort(df["Array_1"])).show()

+-----+------------------+------------------+
| Name|           Array_1|           newSort|
+-----+------------------+------------------+
| John|  [4, 6, 7, 9, 21]|  [4, 6, 7, 9, 21]|
|David|[7, 5, 1, 4, 7, 1]|[1, 1, 4, 5, 7, 7]|
| Mike|   [3, 9, 1, 6, 2]|   [1, 2, 3, 6, 9]|
+-----+------------------+------------------+

#PARTITIONBY
------------
#PARTITIONBY 
df1 = df.write.option("Header", True).mode("overwrite").partitionBy("contract_end_year").csv('dbfs:/FileStore/tables/partitonby')
display(df1)

Partitionby two columns 
-------------------------
x = df.write.option("header", True) \
      .mode("overwrite")
      .partitonby("contract_end_year", "national_team") \
      .csv('dbfs:/FileStore/tables/partitonby')
      
Partitionby number of records
-------------------------------
x = df.write.option("header", True) \
      .option("maxRecordsPerFile", 2000) \
      .mode("overwrite")
      .partitionby("contract_end_year") \
      .csv("dbfs:/FileStore/tables/partitonby")

in the data if the year contains more than 5000 records, it will create one partition, when maxRecorsPerFile is mentioned then it will create more partitions in which each file with 2000 records, so totally we will have 3 parttions for 5000 records data.

## ** HOW TO FIND NUMBER OF RECORDS TO EACH PARTITON **
------------------------------------------------------- 
1. first we need to read the file 
----------------------------------
df = spark.read.format("csv").option("Header", True).load('dbfs:/FileStore/tables/fifa_cleaned.csv')
display(df)

2. know the number of partitions 
--------------------------------
display(df.rdd.getNumPartitions()) 
 -> for the above file the number of partitions is only 2 
 
3. Now find the partitons count
-------------------------------

from pyspark.sql.functions import spark_partition_id

v = df.withColumn("partitions_id", spark_partition_id()).groupby(partitions_id).count()

o/p 
+-----------+-----+
|partitionId|count|
+-----------+-----+
|          0|11818|
|          1| 6136|
+-----------+-----+

4. how to repartition the number of partitions from default
------------------------------------------------------------
v = df.select(df.id,df.age,df.nationality,df.club_team,df.body_type).repartition(6)

5. know the number of partitions
--------------------------------
display(v.rdd.getNumPartitions())

6. now find the partition count()
---------------------------------
from pyspark.sql.functions import spark_partition_id 

f = v.withColumn("new_partition_id", spark_partition_id()).groupBy("new_partition_id").count()

# Null Count of Each Column in Dataframe
----------------------------------------



# Pyspark: Find Top or Bottom N Rows per Group
----------------------------------------------
data_student = [("Michael", "Physics", 80, "P", 90),
                ("Michael", "Chemistry", 67, "P",90),
                ("Michael", "Mathematics", 78, "P",90),
                ("Nancy", "Physics", 30, "F" ,80), 
                ("Nancy", "Chemistry", 59, "p", 80),
                ("Nancy", "Mathematics", 75, "p" ,80),
                ("David", "Physics" ,90,"p",76),
                ("David", "Chemistry", 87, "p", 70),
                ("David", "Mathematics" ,97, "p" ,70),
                ("John", "Physics" ,33, "F",60),
                ("John", "Chemistry", 28, "F", 60), 
                ("John", "Mathematics", 52, "P", 50), 
                ("Blessy", "Physics" ,89,"p", 75) ,
                ("Blessy", "Chemistry", 76, "P", 75),
                ("Blessy","Mathematics", 63, "p", 75)]
Schema = ["name", "Subject", "Mark", "Status", "Attendance"]
df = spark.createDataFrame (data = data_student, schema = Schema)
display(df)

+-------+-----------+----+------+----------+
|   name|    Subject|Mark|Status|Attendance|
+-------+-----------+----+------+----------+
|Michael|    Physics|  80|     P|        90|
|Michael|  Chemistry|  67|     P|        90|
|Michael|Mathematics|  78|     P|        90|
|  Nancy|    Physics|  30|     F|        80|
|  Nancy|  Chemistry|  59|     p|        80|
|  Nancy|Mathematics|  75|     p|        80|
|  David|    Physics|  90|     p|        76|
|  David|  Chemistry|  87|     p|        70|
|  David|Mathematics|  97|     p|        70|
|   John|    Physics|  33|     F|        60|
|   John|  Chemistry|  28|     F|        60|
|   John|Mathematics|  52|     P|        50|
| Blessy|    Physics|  89|     p|        75|
| Blessy|  Chemistry|  76|     P|        75|
| Blessy|Mathematics|  63|     p|        75|


from pyspark.sql.window import Window 
from pyspark.sql.function import col, row_number

windows = Window.partitionBy("name").orderBy(col('Mark').desc())
result = df1.withColumn('row',row_number().over(windows).orderBy("name","row")
display(result)
+-------+-----------+----+------+----------+---+
|   name|    Subject|Mark|Status|Attendance|row|
+-------+-----------+----+------+----------+---+
| Blessy|    Physics|  89|     p|        75|  1|
| Blessy|  Chemistry|  76|     P|        75|  2|
| Blessy|Mathematics|  63|     p|        75|  3|
|  David|Mathematics|  97|     p|        70|  1|
|  David|    Physics|  90|     p|        76|  2|
|  David|  Chemistry|  87|     p|        70|  3|
|   John|Mathematics|  52|     P|        50|  1|
|   John|    Physics|  33|     F|        60|  2|
|   John|  Chemistry|  28|     F|        60|  3|
|Michael|    Physics|  80|     P|        90|  1|
|Michael|Mathematics|  78|     P|        90|  2|
|Michael|  Chemistry|  67|     P|        90|  3|
|  Nancy|Mathematics|  75|     p|        80|  1|
|  Nancy|  Chemistry|  59|     p|        80|  2|
|  Nancy|    Physics|  30|     F|        80|  3|
+-------+-----------+----+------+----------+---+

df1 = result.filter(col('row') =>1)

+-------+-----------+----+------+----------+---+
|   name|    Subject|Mark|Status|Attendance|row|
+-------+-----------+----+------+----------+---+
| Blessy|    Physics|  89|     p|        75|  1|
|  David|Mathematics|  97|     p|        70|  1|
|   John|Mathematics|  52|     P|        50|  1|
|Michael|    Physics|  80|     P|        90|  1|
|  Nancy|Mathematics|  75|     p|        80|  1|
+-------+-----------+----+------+----------+---+

#Greatest vs Least vs Max vs Min
----------------------------------
input_data = [("David", 70,68,89,40,84),
              ("Kevin", 90,67,87,79,74),
              ("Natalia", 66,88,49,65,72) , 
              ("Roger", 78,73,82,89,67),
              ("Michael", 80, 86,69, 78,92)]
schema = ["Student", "Subject_1", "Subject_2", "Subject_3", "Subject_4", "Subject_5"]
df = spark.createDataFrame( input_data, schema)
df.display ()

#Max Value:
-----------
df.agg({"Subject_1":"max", "Subject_2":"max","Subject_3":"max"}).show()

+--------------+--------------+--------------+
|max(Subject_2)|max(Subject_1)|max(Subject_3)|
+--------------+--------------+--------------+
|            88|            90|            89|
+--------------+--------------+--------------+

#Min Value:
-----------
df.agg({"Subject_1":"min", "Subject_2":"min","Subject_3":"min"}).show()
+--------------+--------------+--------------+
|min(Subject_2)|min(Subject_1)|min(Subject_3)|
+--------------+--------------+--------------+
|            67|            66|            49|
+--------------+--------------+--------------+

#DELTA TABLE CREATION 
-----------------------
#  Creating the delta table 

from delta.tables import *
DeltaTable.create(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id",'INT') \
    .addColumn("emp_name",'STRING') \
    .addColumn("gender",'STRING') \
    .addColumn("salary",'INT') \
    .addColumn("Dept",'STRING') \
    .property("description","This is just a demo table") \
    .location("dbfs:/FileStore/CreateTable") \
    .execute()
    
from delta.tables import *
DeltaTable.createIfNotExists(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id",'INT') \
    .addColumn("emp_name",'STRING') \
    .addColumn("gender",'STRING') \
    .addColumn("salary",'INT') \
    .addColumn("Dept",'STRING') \
    .property("description","This is just a demo table") \
    .location("dbfs:/FileStore/CreateTable") \
    .execute()
    
from delta.tables import *
DeltaTable.createOrReplace(spark) \
    .tableName("employee_emo") \
    .addColumn("emp_id",'INT') \
    .addColumn("emp_name",'STRING') \
    .addColumn("gender",'STRING') \
    .addColumn("salary",'INT') \
    .addColumn("Dept",'STRING') \
    .property("description","This is just a demo table") \
    .location("dbfs:/FileStore/CreateTable") \
    .execute()
    
#DELTA SCHEMA MERGE 
--------------------
EXAMPLE: 
-------
from delta.tables import *
DeltaTable.create(spark) \
 .tableName("employee_demo") \
 .addColumn("emp_id","INT") \
 .addColumn("emp_name","STRING") \
 .addColumn("gender","STRING") \
 .addColumn("salary","INT") \
 .addColumn("Dept","STRING") \
 .property("description","table created for demo") \
 .location("/FileStore/tables/delta/path_employee_demo") \
 .execute()
 
 %sql
insert into employee_demo values(100,"Micheal","M",5000,"Sales")

%sql
select * from employee_demo;

from pyspark.sql.types import IntegerType, StringType
employee_data = [(200,"Philips","M",6000,"Marketing","additional")]
employee_schema =StructType([ \
 StructField("emp_id",IntegerType(), False),\
 StructField("emp_name",StringType(), True), \
 StructField("gender",StringType(), True), \
 StructField("salary",IntegerType(), True), \
 StructField("Dept",StringType(), True), \
 StructField("additionalColumn",StringType(), True) ])

-> df = spark.createDataFrame(data=employee_data, schema =employee_schema)
display(df)

-> df.write.format("delta").mode("append").saveAsTable("employee_data") ---> This Command will fail to merge the schema with additional column 

-> df.write.option("mergeSchema", True).format("delta").mode("append").saveAsTable("employee_data")

#DATAFRAME INSERT INTO DELTA TABLE 
----------------------------------

df.write.option("mergeSchema", True).format("delta").mode("append").saveAsTable("employee_demo")

#LEAD and LAG
--------------
example:
-------
simpleData = (("James", "Sales", 3000), \
              ("Micheal", "Sales", 4600), \
              ("Robert", "sales", 4100), \
              ("James","Sales",3000),\
              ("Saif","Sales", 4100), \
              ("Maria","Finanace", 3000), \
              ("Scott", "Finanace", 3300), \
              ("Jen","Finanace", 3900), \
              ("Jeff","Marketing",3000), \
              ("Kumar", "Marketing", 2000))
Columns = ["employee_name","department","salary"]
df = spark.createDataFrame(data =simpleData, schema = Columns)

df.show()

+-------------+----------+------+
|employee_name|department|salary|
+-------------+----------+------+
|        James|     Sales|  3000|
|      Micheal|     Sales|  4600|
|       Robert|     sales|  4100|
|        James|     Sales|  3000|
|         Saif|     Sales|  4100|
|        Maria|  Finanace|  3000|
|        Scott|  Finanace|  3300|
|          Jen|  Finanace|  3900|
|         Jeff| Marketing|  3000|
|        Kumar| Marketing|  2000|
+-------------+----------+------+

from pyspark.sql.window import Window 
from pyspark.sql.functions import row_number 

windowsSpec = Window.partitionBy("department").orderBy("salary")

lag:
-----
from pyspark.sql.functions import lag 
df.withColumn("lag", lag("salary",1).over(windowsSpec)) \
    .show()
    
+-------------+----------+------+----+
|employee_name|department|salary| lag|
+-------------+----------+------+----+
|        Maria|  Finanace|  3000|null|
|        Scott|  Finanace|  3300|3000|
|          Jen|  Finanace|  3900|3300|
|        Kumar| Marketing|  2000|null|
|         Jeff| Marketing|  3000|2000|
|        James|     Sales|  3000|null|
|        James|     Sales|  3000|3000|
|         Saif|     Sales|  4100|3000|
|      Micheal|     Sales|  4600|4100|
|       Robert|     sales|  4100|null|
+-------------+----------+------+----+

from pyspark.sql.functions import lead 
df.withColumn("Lead",lead("salary",1).over(windowsSpec)).show()

+-------------+----------+------+----+
|employee_name|department|salary|Lead|
+-------------+----------+------+----+
|        Maria|  Finanace|  3000|3300|
|        Scott|  Finanace|  3300|3900|
|          Jen|  Finanace|  3900|null|
|        Kumar| Marketing|  2000|3000|
|         Jeff| Marketing|  3000|null|
|        James|     Sales|  3000|3000|
|        James|     Sales|  3000|4100|
|         Saif|     Sales|  4100|4600|
|      Micheal|     Sales|  4600|null|
|       Robert|     sales|  4100|null|
+-------------+----------+------+----+


#MaxOver() Get Max Value of Duplicate Data - MaxOver Window Function
--------------------------------------------------------------------
simpleData = ( (100, "Mobile", 5000, 10), \
               (100, "Mobile", 7000, 7), \
               (200, "Laptop", 20000, 4), \
               (200, "Laptop", 25000,8), \
               (200, "Laptop", 22000, 12))
                                                                      
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
defSchema = StructType ([\
              StructField("Product_id", IntegerType (), False), \
              StructField("Product_name",StringType () ,True), \
              StructField("Price", IntegerType () , True), \
              StructField("DiscountPercent", IntegerType () , True)
               ])
df = spark. createDataFrame (data = simpleData, schema = defSchema).show()
display (df)

+----------+------------+-----+---------------+
|Product_id|Product_name|Price|DiscountPercent|
+----------+------------+-----+---------------+
|       100|      Mobile| 5000|             10|
|       100|      Mobile| 7000|              7|
|       200|      Laptop|20000|              4|
|       200|      Laptop|25000|              8|
|       200|      Laptop|22000|             12|
+----------+------------+-----+---------------+

#STEP1 : here we will be creating a partitionby Product id and find the max values in the columns
--------------------------------------------------------------------------------------------------
from pyspark.sql import Window 
from pyspark.sql.functions import max, col 

WindowSpec = Window.partitionBy("Product_id")

dfMax = (df.withColumn("MaxPrice",max("Price").over(WindowSpec).withColumn("MaxDiscountPrice",max("DiscountPercent").over(WindowSpec)))
display(dfMax)

+----------+------------+-----+---------------+--------+------------------+
|Product_id|Product_name|Price|DiscountPercent|maxPrice|maxDiscountpercent|
+----------+------------+-----+---------------+--------+------------------+
|       100|      Mobile| 5000|             10|    7000|                10|
|       100|      Mobile| 7000|              7|    7000|                10|
|       200|      Laptop|20000|              4|   25000|                12|
|       200|      Laptop|25000|              8|   25000|                12|
|       200|      Laptop|22000|             12|   25000|                12|
+----------+------------+-----+---------------+--------+------------------+

#STEP2: here we will remove the original columns and replace with the max value column
-------------------------------------------------------------------------------------- 
dfSel = dfMax.select(col("Product_id"),col("Product_name"),col("maxPrice").alias("Price"),col("maxDiscountpercent").alias("DiscountPercent")
display(dfSel)

+----------+------------+-----+---------------+
|Product_id|Product_name|Price|DiscountPercent|
+----------+------------+-----+---------------+
|       100|      Mobile| 7000|             10|
|       100|      Mobile| 7000|             10|
|       200|      Laptop|25000|             12|
|       200|      Laptop|25000|             12|
|       200|      Laptop|25000|             12|
+----------+------------+-----+---------------+

#STEP3:
remove_duplicate = dfSel.remove_duplicates()
display(remove_duplicate)

+----------+------------+-----+---------------+
|Product_id|Product_name|Price|DiscountPercent|
+----------+------------+-----+---------------+
|       100|      Mobile| 7000|             10|
|       200|      Laptop|25000|             12|
+----------+------------+-----+---------------+

 # CREATE_MAP FUNCTION
 ------------------------
 from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [ (100, "Mobile", 20000, 10), \
         (200, "Laptop", 85000, 12), \
         (300, "Television", 45000,8), \
         (400, "Monitor", 7000,9), 
         (500, "Headset", 6500, 15) ]

schema = StructType ([
         StructField('ProductID', IntegerType(), True),
         StructField('ProductName', StringType(), True), 
         StructField('UnitPrice', IntegerType(), True),
         StructField('DiscountPercent',IntegerType(),True)
         ])
df = spark.createDataFrame (data=data, schema=schema)

df.display ()

+---------+-----------+---------+---------------+
|ProductID|ProductName|UnitPrice|DiscountPercent|
+---------+-----------+---------+---------------+
|      100|     Mobile|    20000|             10|
|      200|     Laptop|    85000|             12|
|      300| Television|    45000|              8|
|      400|    Monitor|     7000|              9|
|      500|    Headset|     6500|             15|
+---------+-----------+---------+---------------+

Creating the Map function 
-------------------------
from pyspark.sql.functions import col,create_map,lit 
create_map = df.select(col("ProductID"),col("ProductName"),col("UnitPrice"),col("DiscountPercent"),
                       create_map(col("ProductName"),col("UnitPrice")).alias("PriceDict")).show()

display(create_map)

+---------+-----------+---------+---------------+--------------------+
|ProductID|ProductName|UnitPrice|DiscountPercent|           PriceDict|
+---------+-----------+---------+---------------+--------------------+
|      100|     Mobile|    20000|             10|   {Mobile -> 20000}|
|      200|     Laptop|    85000|             12|   {Laptop -> 85000}|
|      300| Television|    45000|              8|{Television -> 45...|
|      400|    Monitor|     7000|              9|   {Monitor -> 7000}|
|      500|    Headset|     6500|             15|   {Headset -> 6500}|
+---------+-----------+---------+---------------+--------------------+


create function and Litfunction
--------------------------------
from pyspark.sql.functions import col, create_map, lit 

litfunction = df.withColumn("PriceDict", create_map(lit("Product_Name"),col("ProductName"),lit("Unit_Price"),col("UnitPrice"))).show(truncate = False)
display(litfunction)

+---------+-----------+---------+---------------+-------------------------------------------------+
|ProductID|ProductName|UnitPrice|DiscountPercent|PriceDict                                        |
+---------+-----------+---------+---------------+-------------------------------------------------+
|100      |Mobile     |20000    |10             |{Product_Name -> Mobile, Unit_Price -> 20000}    |
|200      |Laptop     |85000    |12             |{Product_Name -> Laptop, Unit_Price -> 85000}    |
|300      |Television |45000    |8              |{Product_Name -> Television, Unit_Price -> 45000}|
|400      |Monitor    |7000     |9              |{Product_Name -> Monitor, Unit_Price -> 7000}    |
|500      |Headset    |6500     |15             |{Product_Name -> Headset, Unit_Price -> 6500}    |
+---------+-----------+---------+---------------+-------------------------------------------------+

# HOW TO ENABLE THE DELTA CACHE/ DISK CACHE/ DBIO  
-------------------------------------------------
-> spark.conf.set("spark.databricks.io.cache.enabled", "true")
-> spark.conf.set("spark.databricks.io.cache.enabled")

how to cache table 
--------------------
select * from names;
CACHE select * from names;

# ********* How to read the Excel file **********  ?
---------------------------------------------------
we can't read the excel file like other files, we need to install the libraries in the cluster level or in the notebook level, 
(com.crealytics:spark-excel_2.12:0.13.5) --> is the library to be installed before reading the excel file 

command: 
--------
-> df = spark.read.format("com.crealytics.spark.excel").option("inferschema", False).option("Header", True).option("dataAddress","sheet3!").load("filelocation")

-> df = spark.read.format("com.crealytics.spark.excel").option("inferschema", False).option("Header", True).option("dataAddress","sheet2!").load("filelocation")

-> In case if we want to combine all the excel sheets in to single o/p then we need to create the User defined function and then combine all th files. 

# Handlining Duplicate Data: DropDuplicates vs Distinct
-------------------------------------------------------
#Create Sample DataFrame 
simpleData = (
              (111, "James", 8),
              (222, "Mike", 5), \
              (111, "James", 8), \
              (222, "Mike", 12)
              )
columns= ["id", "name","score"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display ()

+---+-----+-----+
| id| name|score|
+---+-----+-----+
|111|James|    8|
|222| Mike|    5|
|111|James|    8|
|222| Mike|   12|
+---+-----+-----+

#Distinct to remove the Duplicates
----------------------------------- 
df1 = df.distinct().display()

+---+-----+-----+
| id| name|score|
+---+-----+-----+
|111|James|    8|
|222| Mike|    5|
|222| Mike|   12|
+---+-----+-----+

#Distinct with Subset 
--------------------
subset = df.select(['id','name']).distinct().show()
+---+-----+
| id| name|
+---+-----+
|111|James|
|222| Mike|
+---+-----+

#Drop duplicate with out subset 
-------------------------------
df.dropDuplicates().display()

+---+-----+-----+
| id| name|score|
+---+-----+-----+
|111|James|    8|
|222| Mike|    5|
|222| Mike|   12|
+---+-----+-----+

#Drop duplicate with subset 
---------------------------
df.dropDuplicates(['id','name']).display()

+---+-----+-----+
| id| name|score|
+---+-----+-----+
|111|James|    8|
|222| Mike|    5|
+---+-----+-----+

#SELECT VS WITHCOLUMN
-------------------------
withcolumn will degrade the performance, when ever we add new column to the strucuture it will internally create the new dataframe, if we create more than 4 column then the shuffling will take place which is costly and performace bottleneck will take place 

Select operation is recommended, eventhought if we create more number of column the shuffling will take place only once and dataframe is also create only once for all the new columns created. 

Example btn SELCT vs WITHCOLUMN 
-------------------------------
from pyspark.sql.functions import col, concat, lit, current_timestamp
dfSelect = df.select ("*",concat (col ("FirstName"), lit (" "), col("LastName")). alias ("Name"),
                          lit (10) .alias ("BonusPercent"), 
                          (col ("salary")*lit (10)) .alias ("TotalSalary"),
                          current_timestamp() .alias ("DateCreated"))
display (dfSelect)

# SELECT VS WITHCOLUMN

dfwithColumn = df.withColumn ("Name", concat (col ("FirstName"), lit (" "), col ("LastName"))) \
                 .withColumn("BonusPercent",lit(10)) \
                 .withColumn ("TotalSalary",col ("salary") *col ("BonusPercent")) \
                 .withColumn ("DateCreated", current_timestamp ())
display (dfwithColumn)
