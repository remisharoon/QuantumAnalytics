package analytics.spark

import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import com.github.nscala_time.time.Imports._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date

object JaceNiagaraData {
   val KEYSPACE = "quantum_app"
   val METER_READING_TABLE = "meter_readings"
   
  def main(args: Array[String]){
    
    val conf = new SparkConf(true)
      .setAppName("weather_data_from_cassandra")
      .setMaster("local[4]")
      .set("spark.cassandra.connection.host", "172.16.1.163")

    val sc = new SparkContext(conf)

    val meterReadingsRDD = sc.cassandraTable[MeterReading](KEYSPACE, METER_READING_TABLE)
    
    meterReadingsRDD.take(10).foreach(println)
    
//    val meterReadingsDF = meterReadingsRDD.toDF()
    
    val jdbc_url = "jdbc.url=jdbc:mysql://quantum10.czskxl7fmc2z.eu-central-1.rds.amazonaws.com:3306/quantum_app_staging?zeroDateTimeBehavior=convertToNull&connectTimeout=60000&socketTimeout=600000"
    val options = Map(
          "url" -> jdbc_url,
          "dbtable" -> "gateways",
          "user" -> "quantum_test",
          "password" -> "esZaGTg9eDjF3Hz20kvd")
    
//    val df = spark.read.format("jdbc").options(options).load()    
//    
//    val tableDf = sparkSession.read
//          .format("org.apache.spark.sql.cassandra")
//          .options(Map( "table" -> table, "keyspace" -> keyspace))
//          .load()    
    
    
  }
  case class MeterReading(meterid:Integer, value:Double, datapointid:Integer, datetimeepoch:Date)  
}