import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{desc, regexp_extract}

object nasa extends App {

  //logger aparecen en tiempo de ejecucion, distintos niveles de error(debug, info, warning, error, fatal..)
  //getlogger (name) aplica a los paquetes importados que empiezan por org
  //nivel OFF no mostrar error de ningun tipo
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("Nasa").master("local[*]").getOrCreate()

  import spark.implicits._

  //this will produce a dataframe with a single column called value
  val base_df = spark.read.text("C:/NasaSpark/ficheros/*")
  base_df.printSchema()

  //let's look at some of the data
  base_df.show(3, false)
  //print(base_df.count())

  val expresion = """([^(\s)]+)\s([^(\s)]+)\s([^(\s)]+)\s\[(\d{2})\/(\D{3})\/(\d{4})[^"]+\"([^(\")]+)\"\s(\d{3})\s(\d+)"""
  val parsed_df = base_df.select(regexp_extract($"value", expresion, 1).alias("host"),
    regexp_extract($"value", expresion, 2).alias("identify"),
    regexp_extract($"value", expresion, 3).alias("user"),
    regexp_extract($"value", expresion, 4).alias("dia"),
    regexp_extract($"value", expresion, 5).alias("mes"),
    regexp_extract($"value", expresion, 6).alias("anio"),
    regexp_extract($"value", expresion, 7).alias("request"),
    regexp_extract($"value", expresion, 8).cast("int").alias("status"),
    regexp_extract($"value", expresion, 9).cast("int").alias("size"))
  parsed_df.show(5, false)
  parsed_df.printSchema()
  //https://spark.apache.org/docs/2.4.0/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  //parsed_df.write.format("parquet").save("parqueteliminar2")
  //parsed_df.write.saveAsTable("parquet3")
 // val parquet2= spark.read.parquet("parquet2")
  //parquet2.show(5)
  //parquet2.write.partitionBy("anio").parquet("parquetParticionado")
  //val parquet3= spark.read.parquet("spark-warehouse/parquet3")
  //parquet3.show(5)
  parsed_df.groupBy("size").count().select(functions.max("count")).show()

  parsed_df.groupBy("status").count().filter($"status" === 200).show()

  parsed_df.groupBy("request").count().sort(desc("count")).show(10)

  //collec_set elimina duplicados
  //te agrupa por anio y te saca sus host y te cuenta los que hay
  parsed_df.groupBy("anio").agg("host"-> "collect_set", "host" -> "count").show(10)

  val sinNulos=parsed_df.na.drop()
  println("nulos:" + sinNulos.filter($"request".isNull).count())

  //sinNulos.write.parquet("parquet")

}
