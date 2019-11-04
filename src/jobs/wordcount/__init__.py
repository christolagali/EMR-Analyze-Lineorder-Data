
from pyspark.sql import Row

def analyze(sparkcontext,sparksession):

    # Load Customer data from HDFS Path
    
    customer_data  = sparkcontext.textFile('/data/customer.tbl')

    # Perform Transformation on the RDD
    customer_transformed_RDD = customer_data.map(transform_customerData)


    # create a Data Frame
    customerDF = sparksession.createDataFrame(customer_transformed_RDD)

    # saving the data to csv
    customerDF.toPandas().to_csv('customer_qty_data.csv')
	
	
	
	

def transform_customerData(row):
    
    cust_lst = row.split('|')
    
    c_custkey = cust_lst[1][9:18]
    c_name = cust_lst[2]
    c_address = cust_lst[3]
    c_city = cust_lst[4]
    c_nation = cust_lst[5]
    
    return Row(c_custkey=c_custkey,c_name=c_name,c_address=c_address,c_city=c_city,c_nation=c_nation)
