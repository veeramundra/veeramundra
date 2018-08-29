package Spark.Core

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object CreatingRDD_OldVersion extends App {
  
	val sparkconf = new SparkConf()
	sparkconf.setAppName("CreatingFirstRDD")
	sparkconf.setMaster("local")
	
	val sc = new SparkContext(sparkconf)
	
	val l = List(1,2,3,4,5)
	
	val lstRdd = sc.parallelize(l)
	println("Num Of elements in RDD : "+lstRdd.count())
	lstRdd.foreach(println)
	
	val filteredRDD = lstRdd.filter(x=>x>3)
	println("Num Of elements in filteredRDD : "+filteredRDD.count())
	filteredRDD.foreach(println)
	
	
}