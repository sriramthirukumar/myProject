from pyspark.sql.functions import *
from delta import *
def main():
    from pyspark.sql import SparkSession
    print("___Creating Spark Session___")
    spark = SparkSession. builder\
    .master("local[*]")\
    .appName("myProject")\
    .enableHiveSupport()\
    .getOrCreate()

    sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    print("___Reading RDD Data___")
    auctionRDD=sc.textFile("file:///home/hduser/sparkdata/auctiondata")
    print(auctionRDD.take(2))
    print("___Converting RDD to Schema RDD___")
    auctionRDDSchema = auctionRDD.map(lambda x:x.split("~")).map(lambda x:(x[0],float(x[1]),float(x[2]),x[3],int(x[4]),float(x[5]),float(x[6]),x[7],int(x[8])))
    print(auctionRDDSchema.take(2))
    print("___Creating DF___")
    auctionColumns = ["auctionid" , "bid" , "bidtime" , "bidder" , "bidderrate" , "openbid" , "price" , "item" , "daystolive"]
    auctionDF=auctionRDDSchema.toDF(auctionColumns)
    print(auctionDF.show(2,truncate=False))
    print("___Counts of xbox from RDD___")
    print(auctionRDDSchema.filter(lambda x:x[7]=="xbox").count())
    print("___Counts of xbox from DF___")
    print(auctionDF.filter(col("item")=="xbox").count())

    print("___Ending Application___")
if __name__ == '__main__':
    main()