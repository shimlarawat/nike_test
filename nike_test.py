from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.enableHiveSupport() \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .appName("nike_test_app").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

start_date = '$start_dt$'

def get_extra_data(xtra_master_df,xtra_sold_df):
    xtra_data_df = xtra_sold_df.join(xtra_master_df, xtra_sold_df.XM_Ref == xtra_master_df.XM_Key)
    xtra_data_df.select(xtra_data_df.XS_Date,xtra_data_df.XM_Name,xtra_data_df.XS_Quantity,concat_ws(',',xtra_data_df.XS_Loc_x,xtra_data_df.XS_Loc_y).alias('SOLD_Location'))

    return xtra_data_df




if __name__ == "__main__":
    try:
        xtra_master_df = spark.table('xtra_master')
        xtra_sold_df = spark.table('xtra_sold')
        xtra_sold_df = xtra_sold_df.where(xtra_sold_df.XS_date == start_date).dropDuplicates()

        xtra_data_df = get_extra_data(xtra_master_df,xtra_sold_df)

    except Exception as e:
        print("Job failed")
        raise