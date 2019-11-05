
from pyspark.sql import Row

def analyze(sparkcontext,sparksession):
    
    """
        Loading Necessary Files 
        -- lineorder.tbl
        -- supplier.tbl
        -- customer.tbl
    """
    # Load Lineorder data from HDFS Path
    lineorder_data = sparkcontext.textFile('/data/lineorder.tbl')

    # Load Supplier Data from HDFS Path
    supplier_data = sparkcontext.textFile('/data/supplier.tbl')

    # Load Customer data from HDFS Path
    customer_data  = sparkcontext.textFile('/data/customer.tbl')


    """
        Caching the RDD for future use
    """
    lineorder_data.cache()
    supplier_data.cache()
    customer_data.cache()


    """
        Applying transofrmations to each of the RDD's
        Once Transformed the RDD's will be converted into a DataFrame for Spark SQL queries
    """

    # transforming lineorder data
    linedata = lineorder_data.map(transform_linedata)

    # transforming customer data
    customer_transformed_RDD = customer_data.map(transform_customerData)

    # transforming Supplier data
    suppdata = supplier_data.map(transform_suppdata)


    # Create a Data Frame
    line_df = sparksession.createDataFrame(linedata)

    # Convert Supplier list to Data Frame
    supp_df = sparksession.createDataFrame(suppdata)

    # create a Data Frame
    customerDF = sparksession.createDataFrame(customer_transformed_RDD)

    # saving the data to csv
    #customerDF.toPandas().to_csv('customer_qty_data.csv')
    #supp_df.toPandas().to_csv('supplier_data.csv')

    """
        Registering temp tables for each dataset.
        SQL Queries will be fored against these temp tables
    """

    # lineorder tmp table
    createTempTable(line_df,'line_tbl')

    # Supplier tmp table
    createTempTable(supp_df,'supp_tbl')

    #customer tmp table
    createTempTable(customerDF,'customer_tbl')

    """
        Define SQL Queries that are fired against Hive Tables and export the results as csv files.
    """

    # Discount provided per city where Shipping Mode = SHIP
    query = 'SELECT a.s_city,sum(b.lo_discount) FROM supp_tbl a, line_tbl b where a.s_suppkey = b.lo_suppkey and b.lo_shipmode = "SHIP" group by a.s_city'
    exportData(query,'discount_by_city.csv',sparksession,sparkcontext)


    # Discount by nation for SHIP as shipping method
    query = 'SELECT a.s_nation,sum(b.lo_discount) FROM supp_tbl a, line_tbl b where a.s_suppkey = b.lo_suppkey and b.lo_shipmode = "SHIP" group by a.s_nation'
    exportData(query,'discount_by_nation.csv',sparksession,sparkcontext)

    # Quantity and total Order Price aggregated by each customer 
    query = 'select b.c_custkey,SUM(a.lo_qty), SUM(a.lo_ordertotalprice) from line_tbl a, customer_tbl b where a.lo_custkey = b.c_custkey group by b.c_custkey order by b.c_custkey'
    exportData(query,'customer_qty_data.csv',sparksession,sparkcontext)

    
	
	
"""
    Transformation Methods to:
    1. split each row (text format)
    2. Perform data type conversion
    3. Return a Row() object which is used to create a DataFrame
"""

"""
    Method to Transform Customer.tbl data
    Param: row: each text row from the RDD
"""
def transform_customerData(row):
    
    cust_lst = row.split('|')
    
    c_custkey = cust_lst[1][9:18]
    c_name = cust_lst[2]
    c_address = cust_lst[3]
    c_city = cust_lst[4]
    c_nation = cust_lst[5]
    
    return Row(c_custkey=c_custkey,c_name=c_name,c_address=c_address,c_city=c_city,c_nation=c_nation)


"""
    Method to Transform lineorder.tbl data
    Param: row: each text row from the RDD
"""
def transform_linedata(row):
    
    lst_row = row.split('|')
    
    lo_orderkey = int(lst_row[0])
    lo_linenumber = int(lst_row[1])
    
    # customer key
    lo_custkey = lst_row[2].zfill(9)
    
    # Supplier key
    lo_suppkey = lst_row[4].zfill(9)
     
    lo_shippriority = lst_row[6]
    lo_qty = int(lst_row[7])
    
    lo_extendprice = int(lst_row[9])
    lo_ordertotalprice = int(lst_row[10])
    
    lo_discount = int(lst_row[12])
    lo_revenue = int(lst_row[13])
    
    lo_tax = int(lst_row[14])
    lo_shipmode = lst_row[16]
    
    return Row(lo_orderkey=lo_orderkey,lo_linenumber=lo_linenumber,lo_custkey=lo_custkey,lo_suppkey=lo_suppkey,lo_shippriority=lo_shippriority,
              lo_qty=lo_qty,lo_extendprice=lo_extendprice,lo_ordertotalprice=lo_ordertotalprice,
              lo_discount=lo_discount,lo_revenue=lo_revenue,lo_tax=lo_tax,lo_shipmode=lo_shipmode)



"""
    Method to Transform supplier.tbl data
    Param: row: each text row from the RDD
"""
def transform_suppdata(row):
    
    lst_row = row.split('|')
    
    s_suppkey = lst_row[1][9:18]
    s_address = lst_row[2]
    s_city = lst_row[3]
    
    s_nation = lst_row[4]
    s_region = lst_row[5]
    
    return Row(s_suppkey=s_suppkey,s_address=s_address,s_city=s_city,s_nation=s_nation,
              s_region=s_region)



"""
    Method that takes in the following params:
    1. dataframe: DataFrame that will be used to call registerTempTable() method
    2. tablename: Name of the temp table that needs to be created
"""
def createTempTable(dataframe,tablename):

    # Using the DataFrame to create a Hive Temp Table against which SQL queries can be fired
    dataframe.registerTempTable(tablename)


"""
    Method that takes in the following params:
    1. query: DataFrame that will be used to call registerTempTable() method
    2. filename: Name of the temp table that needs to be created
    3. sparksession: Spark Session that is defined in the main program
    4. sparkcontext: Spark Context that is defined in the main program
"""
def exportData(query,filename,sparksession,sparkcontext):

    # Discount by city for SHIP as shipping method
    querydf = sparksession.sql(query).collect()

    resultDF = sparksession.createDataFrame(sparkcontext.parallelize(querydf))

    # We leverge the toPandas() method to convert the Spark DataFrame into Pandas DataFrame and save it.
    resultDF.toPandas().to_csv(filename)



