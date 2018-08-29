package Spark.SQL

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object SQLUsingNewVersion extends App {
	Logger.getLogger("org").setLevel(Level.ERROR);
	
	val ss = SparkSession.builder().appName("SQLUsingNewVersion").master("local[*]").getOrCreate()
	val rdd = ss.sparkContext.parallelize(List(1,2,3,4,5),1)
	
//	case class RowSchema(Numbers:Int);
//	val rowRDDCase = rdd.map(x=>RowSchema(x))
//	val CaseDF = ss.createDataFrame(rowRDDCase, )
	
	
	val schema = StructType(
			StructField("Number",IntegerType,true)::Nil
)

val rowRDD = rdd.map(x=>Row(x))

val df = ss.createDataFrame(rowRDD,schema)
df.printSchema()
df.show()
	
	
  
}