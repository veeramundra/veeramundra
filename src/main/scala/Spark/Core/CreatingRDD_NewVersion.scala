package Spark.Core

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.sun.org.apache.xalan.internal.xsltc.compiler.ForEach

object CreatingRDD_NewVersion extends App {
	
	Logger.getLogger("org").setLevel(Level.ERROR);
	
	println("********Creating RDD From an Array using Spark Session Object********")
  
	val ss = SparkSession.builder().appName("Veera").master("local[*]").getOrCreate()
  val array = Array(1,2,3,4,5)
  val arrRDD = ss.sparkContext.parallelize(array, 2)
  
  println("Num of elements in RDD : "+arrRDD.count())
  
  arrRDD.foreach(println)
  
  println("********Creating RDD From a Text File********")
  
  val filepath = "C:/Veera-Backup/BigData/SpUsingScala/SparkExample/src/main/resources/testdata/testdata1.txt"
  val fileRDD = ss.sparkContext.textFile(filepath, 1)
  
  println("Num of Line in File" + fileRDD.count())
  
  fileRDD.collect.foreach(println)
  
  
  
}