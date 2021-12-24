from pyspark import SparkContext
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import boto3
from pyspark import SparkContext
from pyspark.shell import spark
import sys
from datetime import date
from datetime import timedelta
from pyspark.sql.functions import when


####TODO Add the harcoded IAM ,database Role and Database Name in Varibales using CDK ######
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

glueContext = GlueContext(SparkContext.getOrCreate())

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

today = date.today()
print("date is today " + str(today) )
report_date = today - timedelta(days=1)
print("report date is: ", report_date)

order_nonIntegrated_ds = glueContext.create_dynamic_frame.from_catalog(database = "s3-webcrawler",redshift_tmp_dir = args["TempDir"], table_name = "semi_automated_pantomath_prod_reporting_daily_sales_non_integrated", transformation_ctx = "datasource()")
order_nonIntegrated_df = order_nonIntegrated_ds.toDF()

nonIntegrated_df = order_nonIntegrated_df.select('channel_name' , 'brand_name' , 'order_date' , 'gmv', 'qty')
nonIntegrated_df.createOrReplaceTempView("view_nonIntegrated_df")


###########################################################
### Loading Data from tables from  Non integrated Channels
###########################################################


  #### Getting data for period D0-D6 from non integrated order data  ####

nonIntegratedAllPeriod_SQL = spark.sql(f"""
select lower(channel_name) as channel,
lower(brand_name) as brands,
 CASE order_date
 WHEN  date(date_add(date('{report_date}'),0)) THEN date(date_add(date('{report_date}'),0))
 WHEN  date(date_add(date('{report_date}'),-1)) THEN date(date_add(date('{report_date}'),-1))
 WHEN  date(date_add(date('{report_date}'),-2)) THEN date(date_add(date('{report_date}'),-2))
 WHEN  date(date_add(date('{report_date}'),-3)) THEN date(date_add(date('{report_date}'),-3))
 WHEN  date(date_add(date('{report_date}'),-4)) THEN date(date_add(date('{report_date}'),-4))
 WHEN  date(date_add(date('{report_date}'),-5)) THEN date(date_add(date('{report_date}'),-5))
 WHEN  date(date_add(date('{report_date}'),-6)) THEN date(date_add(date('{report_date}'),-6))
 ELSE date(order_date)
 END as period ,
 sum(gmv) as gmv,
 sum(qty) as unit_sold,
date(date_add(date('{report_date}'),0)) as actual_date
from view_nonIntegrated_df
where date(order_date) > date(date_add(date('{report_date}'),-6))
group by 1,2,3
""")
print("Order Non Integrated  All Period  calculated ")
#nonIntegratedAllPeriod_SQL.show()


#### Getting data for  Month Till Date  from non integrated order data  ####

nonIntegratedMTD_SQL = spark.sql(f"""select lower(channel_name) as channel,
lower(brand_name) as brands,
'MTD' as period,
sum(gmv) as gmv,
sum(qty) as unit_sold,
date(date_trunc('month',date('{report_date}'))) as actual_date
from view_nonIntegrated_df
where date(order_date) between date(date_trunc('month',date('{report_date}'))) and date('{report_date}')
group by 1,2,3
""")
#nonIntegratedMTD_SQL.show()
print("Order Non Integrated  MTD calculated ")


#### Getting data for  Average Month Till Date  from non integrated order data  ####

nonIntegratedAVGMTD_SQL = spark.sql(f"""select lower(channel_name) as channel,
lower(brand_name) as brands,
'AVG MTD' as period,
sum(gmv)/count(qty) as gmv,
sum(qty)/count(distinct date(order_date)) as unit_sold,
date(date_trunc('month',date('{report_date}'))) as actual_date
from view_nonIntegrated_df
where date(order_date) between date(date_trunc('month',date('{report_date}'))) and date('{report_date}')
group by 1,2,3
""")

#nonIntegratedAVGMTD_SQL.show()
print("Order Non Integrated AVG MTD calculated ")


###########################################################
### Loading Data from tables from   integrated Channels
###########################################################

order_Integrated_ds = glueContext.create_dynamic_frame.from_catalog(database = "s3-webcrawler", redshift_tmp_dir = args["TempDir"],table_name = "semi_automated_pantomath_prod_analytics_f_order", transformation_ctx = "datasource()")
order_Integrated_df = order_Integrated_ds.toDF()
#order_nonIntegrated_ds.show()
orderIntegrated_df = order_Integrated_df.select('channel_name' , 'brand_name' , 'order_date' , 'total_price', 'quantity','channel_order_sku_id')
orderIntegrated_df.createOrReplaceTempView("view_integrated_df")


  #### Getting data for period D0-D6 from non integrated order data  ####

orderIntegratedAllPeriod_SQL = spark.sql(f"""
select lower(channel_name) as channel,
lower(brand_name) as brands,
 CASE order_date
 WHEN  date(date_add(date('{report_date}'),0)) THEN date(date_add(date('{report_date}'),0))
 WHEN  date(date_add(date('{report_date}'),-1)) THEN date(date_add(date('{report_date}'),-1))
 WHEN  date(date_add(date('{report_date}'),-2)) THEN date(date_add(date('{report_date}'),-2))
 WHEN  date(date_add(date('{report_date}'),-3)) THEN date(date_add(date('{report_date}'),-3))
 WHEN  date(date_add(date('{report_date}'),-4)) THEN date(date_add(date('{report_date}'),-4))
 WHEN  date(date_add(date('{report_date}'),-5)) THEN date(date_add(date('{report_date}'),-5))
 WHEN  date(date_add(date('{report_date}'),-6)) THEN date(date_add(date('{report_date}'),-6))
 ELSE date(order_date)
 END as period ,
sum(total_price) as gmv,
sum(quantity) as unit_sold,
date(date_add(date('{report_date}'),0)) as actual_date
from view_integrated_df
where date(order_date) > date(date_add(date('{report_date}'),-6)) and channel_order_sku_id not in
(
select channel_order_sku_id from view_integrated_df where (lower(brand_name) in ('anubhutee','ishin') and lower(channel_name) like '%flip%' or (lower(brand_name)='lilpicks' and lower(channel_name)='firstcry') or (lower(brand_name)='ishin' and lower(channel_name) like '%myn%' and date(order_date)<=date('2021-12-01'))))
group by 1,2,3
""")

#orderIntegratedAllPeriod_SQL.show()
print(" Order Integrated All period calculated ")

  #### Getting data for period D0-D6 from Integrated order data  ####

orderIntegratedMTD_SQL = spark.sql(f"""select lower(channel_name) as channel,
lower(brand_name) as brands,
'MTD' as period,
sum(total_price) as gmv,
sum(quantity) as unit_sold,
date(date_trunc('month',date('{report_date}'))) as actual_date
from view_integrated_df
where date(order_date) between date(date_trunc('month',date('{report_date}'))) and date('{report_date}')  and channel_order_sku_id not in (
select channel_order_sku_id from view_integrated_df where (lower(brand_name) in ('anubhutee','ishin') and lower(channel_name) like '%flip%' or (lower(brand_name)='lilpicks' and lower(channel_name)='firstcry') or (lower(brand_name)='ishin' and lower(channel_name) like '%myn%' and date(order_date)<=date('2021-12-01'))))
group by 1,2,3
""")
#orderIntegratedMTD_SQL.show()
print(" Order Integrated MTD calculated ")

#### Getting data for MTD from Integrated order data  ####

orderIntegratedAVGMTD_SQL = spark.sql(f"""select lower(channel_name) as channel,
lower(brand_name) as brands,
'AVG MTD' as period,
sum(total_price)/count(quantity) as gmv,
sum(quantity)/count(distinct date(order_date)) as unit_sold,
date(date_trunc('month',date('{report_date}'))) as actual_date
from view_integrated_df
where date(order_date) between date(date_trunc('month',date('{report_date}'))) and date('{report_date}') and channel_order_sku_id not in (
select channel_order_sku_id from view_integrated_df where (lower(brand_name) in ('anubhutee','ishin') and lower(channel_name) like '%flip%' or (lower(brand_name)='lilpicks' and lower(channel_name)='firstcry') or (lower(brand_name)='ishin' and lower(channel_name) like '%myn%' and date(order_date)<=date('2021-12-01'))))
group by 1,2,3
""")

#orderIntegratedAVGMTD_SQL.show()
print(" Data orderIntegrated for AVGMTD calculated ")

#### Starting with Union of multiple data frames #####

periodDF = nonIntegratedAllPeriod_SQL.union(orderIntegratedAllPeriod_SQL)  ##Imitating Union
mtdDF = nonIntegratedMTD_SQL.union(orderIntegratedMTD_SQL)    #Imitating Union
avgMtdDF = nonIntegratedAVGMTD_SQL.union(orderIntegratedAVGMTD_SQL)#Imitating Union

mtdCombinedDf = mtdDF.union(avgMtdDF).distinct()  ##Imitating Union ALL
misDf=periodDF.union(mtdCombinedDf).distinct() ##Imitating Union ALL

print("Schema for the final Data Frame is :")

#misDf = misDf.filter(misDf.actual_date!=(to_date(today,'yyyy-MM-dd')))

misDf.filter(misDf("actual_date").lt(today))


misDf.printSchema()
print("#####################################")

print("Data Union completed")


# misDf.withColumn("period", when(misDf.period == (report_date- timedelta(days=0)),"DO") \
#       .when(misDf.period == (report_date- timedelta(days=1)),"D1") \
#       .when(misDf.period == (report_date- timedelta(days=2)),"D2") \
#       .when(misDf.period == (report_date- timedelta(days=3)),"D3") \
#       .when(misDf.period == (report_date- timedelta(days=4)),"D4") \
#       .when(misDf.period == (report_date- timedelta(days=5)),"D5") \
#       .when(misDf.period == (report_date- timedelta(days=6)),"D6") \
#       .otherwise(misDf.period))

# misDf.explain()

###############################################
### Saving data to redhsift  :-mis report table
###############################################

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(misDf, glueContext, 'review'),
    catalog_connection="redshift-mensa-dwh",
    connection_options={
        "dbtable": "public.mis_report",
        "database": "pantomath_prod",
        "aws_iam_role": "arn:aws:iam::092621740321:role/RedshiftClusterRole"
    },
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="exported_df"
)

print("saving data to Database  completed")









