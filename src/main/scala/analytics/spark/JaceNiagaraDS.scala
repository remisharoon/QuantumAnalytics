package analytics.spark

import org.apache.spark.sql.cassandra._
//import com.mysql.jdbc.Driver._

import java.sql.{ Connection, DriverManager, SQLException }
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date


object JaceNiagaraDS {
   val KEYSPACE = "quantum_app"
   val METER_READING_TABLE = "meter_readings"
   
  def main(args: Array[String]){
    Class.forName("com.mysql.jdbc.Driver").newInstance
    val conf = new SparkConf(true)
      .setAppName("weather_data_from_cassandra")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "172.16.1.163")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()    

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._      
      
    // set params for all clusters and keyspaces
    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))
    
    // set params for the particular cluster
    spark.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("172.16.1.163") ++ CassandraConnectorConf.ConnectionPortParam.option(12345))
    spark.setCassandraConf("Cluster2", CassandraConnectorConf.ConnectionHostParam.option("172.16.1.163"))
    
    // set params for the particular keyspace 
    spark.setCassandraConf("Cluster1", "ks1", ReadConf.SplitSizeInMBParam.option(128))
//    spark.setCassandraConf("Cluster1", "ks2", ReadConf.SplitSizeInMBParam.option(64))
//    spark.setCassandraConf("Cluster2", "ks3", ReadConf.SplitSizeInMBParam.option(80))      
      
    val meterReadingsRDD = sc.cassandraTable[MeterReading](KEYSPACE, METER_READING_TABLE)
    
    meterReadingsRDD.take(10).foreach(println)
    
//    val meterReadingsDF = meterReadingsRDD.toDF()
    
    val jdbc_url = "jdbc.url=jdbc:mysql://quantum10.czskxl7fmc2z.eu-central-1.rds.amazonaws.com:3306/quantum_app_staging?zeroDateTimeBehavior=convertToNull&connectTimeout=60000&socketTimeout=600000"
    val options = Map(
          "url" -> jdbc_url,
          "dbtable" -> "gateways",
          "driver" -> "com.mysql.jdbc.Driver",
          "user" -> "quantum_test",
          "password" -> "esZaGTg9eDjF3Hz20kvd")
    
    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)          
          
    val df = spark.read.format("jdbc").options(options).load()    
//    
//    val tableDf = sparkSession.read
//          .format("org.apache.spark.sql.cassandra")
//          .options(Map( "table" -> table, "keyspace" -> keyspace))
//          .load()    
    
    val optionsCass = Map(
        "table" -> "words",
        "keyspace" -> "test" ,
        "cluster" -> "ClusterOne"
        )       
          
    val lastdf = spark.read.format("org.apache.spark.sql.cassandra").options(optionsCass).load() // This Dataset will use a spark.cassandra.input.split.size of 48    
          
    lastdf.printSchema()      
  }
  case class MeterReading(meterid:Integer, value:Double, datapointid:Integer, datetimeepoch:Date)    
}