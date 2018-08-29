package Spark.SQL

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object FirstSparkSQLJob_OldVersion extends App {
  Logger.getLogger("org").setLevel(Level.ERROR);
	//val sparkSession = SparkSession.builder().appName("FirstSparkSQLApp").master("local[*]").getOrCreate()
  
  val sparkConf = new SparkConf().setAppName("FirstSparkSQLJob_OldVersion").setMaster("local")
  val sc = new SparkContext(sparkConf)
  
  val sQc = new SQLContext(sc)
  
  val RDD = sc.parallelize(Array(1,2,3,4,5))
	
  val schema = StructType(
  		StructField("Numbers",IntegerType, false)::Nil
	)
  		
  val rowRDD = RDD.map(line=>Row(line)) // Converted each object in to a row object
  
  val df = sQc.createDataFrame(rowRDD, schema)
  df.printSchema()
  df.show()
  
}