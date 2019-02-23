package com.learning
import org.apache.spark._
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, BooleanType,TimestampType }

object AnalyzeFireSFOData {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
      lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

    

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Analyze the City of San Francisco's Open Data")
    
    val spark = SparkSession
  .builder()  
  .config(sparkConf)
  .getOrCreate()


    // Read each line of input data
    val sqlContext = new SQLContext(sc)

    val fireDF = spark.read.
      option("delimiter", ",").
      option("header", "true").
      csv("hdfs://localhost:8020/user/shadoop/sanfrancisco/firedepartment/Fire_Department_Calls_for_Service.csv")
      .cache();
     val IncidentsDF = spark.read.
      option("delimiter", ",").
      option("header", "true").
      option("inferSchema", "true").
      csv("hdfs://localhost:8020/udemy/practise/input/Fire_Incidents.csv")
      .withColumnRenamed("Incident Number","IncidentNumber" )
      .cache();

    val fireSchema = StructType(Array(
      StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDate", StringType, true),
      StructField("ReceivedDtTm", StringType, true),
      StructField("EntryDtTm", StringType, true),
      StructField("DispatchDtTm", StringType, true),
      StructField("ResponseDtTm", StringType, true),
      StructField("OnSceneDtTm", StringType, true),
      StructField("TransportDtTm", StringType, true),
      StructField("HospitalDtTm", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvaliableDtTm", StringType, true),
      StructField("Address", StringType, true),
      StructField("City", StringType, true),
      StructField("ZipcodeofIncident", IntegerType, true),
      StructField("Battalion", StringType, true),
      StructField("StationArea", StringType, true),
      StructField("Box", StringType, true),
      StructField("OriginalPriority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", IntegerType, true),
      StructField("ALSUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumberofAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("Unitsequencecalldispatch", IntegerType, true),
      StructField("FirePreventionDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("NeighborhoodDistrict", StringType, true),
      StructField("Location", StringType, true),      
      StructField("RowID", StringType, true)));
    
    val dsf = spark.read
    .option("header", "true") // Use first line of all files as header
    .option("delimiter", ",")
    .schema(fireSchema)
    .csv("hdfs://localhost:8020/user/shadoop/sanfrancisco/firedepartment/Fire_Department_Calls_for_Service.csv");
    val format1 = new SimpleDateFormat("MM/dd/yyyy")
    val format2 = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa")


    import spark.implicits._

    dsf.printSchema();
    //val dfQuestions = dsf.select(unix_timestamp($"callDate", "dd/MM/yyyy").cast("timestamp"));
    
    val dfQuestions =dsf.withColumn("CallDateTS", unix_timestamp($"CallDate", "MM/dd/yyyy").cast("timestamp")).drop("CallDate")
                        .withColumn("WatchDateTS", unix_timestamp($"WatchDate", "MM/dd/yyyy").cast("timestamp")).drop("WatchDate")
                        .withColumn("ReceivedDtTmTS", unix_timestamp($"ReceivedDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("ReceivedDtTm")
                        .withColumn("EntryDtTmTS", unix_timestamp($"EntryDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("EntryDtTm")
                        .withColumn("DispatchDtTmTS", unix_timestamp($"DispatchDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("DispatchDtTm")
                        .withColumn("ResponseDtTmTS", unix_timestamp($"ResponseDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("ResponseDtTm")
                        .withColumn("OnSceneDtTmTS", unix_timestamp($"OnSceneDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("OnSceneDtTm")
                        .withColumn("TransportDtTmTS", unix_timestamp($"TransportDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("TransportDtTm")
                        .withColumn("HospitalDtTmTS", unix_timestamp($"HospitalDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("HospitalDtTm")
                        .withColumn("AvaliableDtTmTS", unix_timestamp($"AvaliableDtTm", "MM/dd/yyyy hh:mm:ss aa").cast("timestamp")).drop("AvaliableDtTm")
                         
//     val dfQuestions = dfQuestionsCSV.select(
// dfQuestionsCSV.col("id").cast("integer"),

    
    //dfQuestions.show();
   // dfQuestions.select(year(dfQuestions("CallDateTS"))).distinct().orderBy(dfQuestions("CallDateTS")).show();
   // dfQuestions.filter("year(CallDateTS)==2017").filter("dayofyear(CallDateTS)>=358").select(dayofyear(dfQuestions("CallDateTS"))).distinct()
   // .orderBy(dayofyear(dfQuestions("CallDateTS"))).show();
//     println(dfQuestions.filter("year(CallDateTS)==2016").filter("dayofyear(CallDateTS)>=180").filter("dayofyear(CallDateTS)<=187")
//    .groupBy(dayofyear(dfQuestions("CallDateTS"))).count().orderBy(dayofyear(dfQuestions("CallDateTS"))));
//    dfQuestions.write.format("parquet").save("hdfs://localhost:8020/user/shadoop/sanfrancisco/firedepartment/parquet");
    //dfQuestions.filter($"age" > 21).show()
    
     dfQuestions.createOrReplaceTempView("fireServiceVIEW");
     spark.catalog.cacheTable("fireServiceVIEW");
     val fireServiceDF=spark.table("fireServiceVIEW")
     val result = spark.sql("SELECT NeighborhoodDistrict,COUNT(NeighborhoodDistrict) AS Neighborhood_Count FROM fireServiceVIEW WHERE year(CallDateTS)==2015 GROUP BY NeighborhoodDistrict ORDER BY  Neighborhood_Count");
     result.show();
     val joinedDF=fireServiceDF.join(IncidentsDF,fireServiceDF("IncidentNumber")===IncidentsDF("IncidentNumber"));
     joinedDF.count();
     joinedDF.show(3);
     joinedDF.filter("year(CallDateTS)==2015").filter($"NeighborhoodDistrict"==="Tenderloin").groupBy("Primary Situation")
     .count().orderBy(desc("count")).show(10);

     

        
   // val fireServiceCallsServiceDF = dfQuestions.withColumn("Year",year(dfQuestions("CallDateTS")))
//   println(dsf.columns);  (unix_timestamp($"BIRTHDT","MM-dd-yyyy").cast("date")) &&


//   println(dsf.count());
//   dsf.select("CallType").show(5);
//   dsf.select("CallType").distinct().show(35, false);
//   //how many incidents of each call type were there
//   dsf.select("CallType").groupBy("CallType").count().orderBy(desc("CallType") );
   
  // val fireServiceCallsServiceDF=dsf.col("callDate").cast("timestamp");
    // dfQuestionsCSV.col("creation_date").cast("timestamp"),

//   .withColumn("WatchDateTS", dsf("WatchDate").cast("timestamp")).drop("WatchDate")
//   .withColumn("ReceivedDtTmTS", dsf("ReceivedDtTm").cast("timestamp")).drop("ReceivedDtTm")
//   .withColumn("EntryDtTmTS", dsf("EntryDtTm").cast("timestamp")).drop("EntryDtTm")
//   .withColumn("DispatchDtTmTS", dsf("DispatchDtTm").cast("timestamp")).drop("DispatchDtTm")
//   .withColumn("ResponseDtTmTS", dsf("ResponseDtTm").cast("timestamp")).drop("ResponseDtTm")
//   .withColumn("OnSceneDtTmTS", dsf("OnSceneDtTm").cast("timestamp")).drop("OnSceneDtTm")
//   .withColumn("TransportDtTmTS", dsf("TransportDtTm").cast("timestamp")).drop("TransportDtTm")
//   .withColumn("HospitalDtTmTS", dsf("HospitalDtTm").cast("timestamp")).drop("HospitalDtTm")
//   .withColumn("AvaliableDtTmTS", dsf("AvaliableDtTm").cast("timestamp")).drop("AvaliableDtTm");
   
              
  //fireServiceCallsServiseDF.printSchema();
  //ireServiceCallsServiseDF.show();
//  def sanitize(input: String): String = s"'$input'"

  //calculate how many distinct years of data in the CSV file
 // val year = data.withColumn("Year",year(fireServiceCallsServiseDF("CallDateTS")))


  //fireServiceCallsServiseDF.select(year(fireServiceCallsServiseDF(sanitize("CallDateTS")))).show();
 // Cannot resolve column name "CallDateTS" among (year(CallDateTS));


  }

}