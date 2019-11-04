
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
    supp_df.toPandas().to_csv('supplier_data.csv')
	
	
	
	

def transform_customerData(row):
    
    cust_lst = row.split('|')
    
    c_custkey = cust_lst[1][9:18]
    c_name = cust_lst[2]
    c_address = cust_lst[3]
    c_city = cust_lst[4]
    c_nation = cust_lst[5]
    
    return Row(c_custkey=c_custkey,c_name=c_name,c_address=c_address,c_city=c_city,c_nation=c_nation)



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




def transform_suppdata(row):
    
    lst_row = row.split('|')
    
    s_suppkey = lst_row[1][9:18]
    s_address = lst_row[2]
    s_city = lst_row[3]
    
    s_nation = lst_row[4]
    s_region = lst_row[5]
    
    
    return Row(s_suppkey=s_suppkey,s_address=s_address,s_city=s_city,s_nation=s_nation,
              s_region=s_region)