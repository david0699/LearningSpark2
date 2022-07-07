package exercises

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat,lit, lower, ltrim, rank, regexp_extract, regexp_replace,substring, to_date, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SparkSession}

import scala.io.Source

object projectTests {
  def doPrueba(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._

    val entity = "pagares"
    val parameters = sparkSession.createDataFrame(Seq(
      ("recibos_01.csv","recibos","CSV",";","1",false,"StringType","id",false,"",""),
      ("recibos_01.csv","recibos","CSV",";","2",false,"StringType","nombre",false,"",""),
      ("recibos_01.csv","recibos","CSV",";","3",false,"StringType","apellidos",false,"",""),
      ("recibos_01.csv","recibos","CSV",";","5",false,"StringType","empleo",false,"",""),
      ("recibos_01.csv","recibos","CSV",";","4",false,"IntegerType","jornada",false,"",""),
      ("facturas_01.txt","facturas","ANCHO FIJO","","1",false,"StringType","id",false,"1","3"),
      ("facturas_01.txt","facturas","ANCHO FIJO","","2",false,"StringType","nombre",false,"4","20"),
      ("facturas_01.txt","facturas","ANCHO FIJO","","3",false,"StringType","apellidos",false,"24","20"),
      ("facturas_01.txt","facturas","ANCHO FIJO","","4",false,"IntegerType","jornada",false,"44","5"),
      ("facturas_01.txt","facturas","ANCHO FIJO","","5",false,"StringType","empleo",false,"49","30"),
      ("pagares_01.json","pagares","JSON","","1",false,"StringType","id",false,"",""),
      ("pagares_01.json","pagares","JSON","","2",false,"StringType","persona.nombre",false,"",""),
      ("pagares_01.json","pagares","JSON","","3",false,"StringType","persona.apellidos",false,"",""),
      ("pagares_01.json","pagares","JSON","","4",false,"IntegerType","jornada",false,"",""),
      ("pagares_01.json","pagares","JSON","","5",false,"StringType","empleo",false,"",""),
      ("rentas_01.yaml","rentas","JSON","","1",false,"StringType","id",false,"",""),
      ("rentas_01.yaml","rentas","JSON","","2",false,"StringType","persona.nombre",false,"",""),
      ("rentas_01.yaml","rentas","JSON","","3",false,"StringType","persona.apellidos",false,"",""),
      ("rentas_01.yaml","rentas","JSON","","4",false,"IntegerType","jornada",false,"",""),
      ("rentas_01.yaml","rentas","JSON","","5",false,"StringType","empleo",false,"","")
    )).toDF("file_name","entity","format","sep","order","header","data_type","name","nullable","inicio","longitud")
      .filter($"entity".equalTo(entity))

    val sep = parameters.select($"sep").first().getString(0)
    val format = parameters.select(lower($"format")).first().getString(0)
    val fileName = parameters.select($"file_name").first().getString(0)
    val options = Map(("header","false"),if(sep.isEmpty) ("","") else ("sep",sep))

    val tableSchema = if(format.equals("ancho fijo")) StructType(Array(StructField("col",StringType)))
      else StructType(parameters
      .orderBy($"order")
      .select($"name",$"data_type",$"nullable")
      .collect()
      .map(row =>
        StructField(row.getString(0)
          ,if(row.getString(1).equals("StringType")) StringType else IntegerType
          ,row.getBoolean(2)
        )))

    val df = format match {
      case "csv" => sparkSession.read.options(options).schema(tableSchema).csv(s"src/main/resources/prueba/$fileName")

      case "ancho fijo" =>
        val rawDf = sparkSession.read.options(options).csv(s"/luca/in/facturas/$fileName")
        val mapColumns = parameters.select($"name",$"inicio",$"longitud").collect().map(row=>row.getString(0)->(row.getString(1),row.getString(2))).toMap

        mapColumns.keys.foldLeft(rawDf){
          (df,k) => df.withColumn(k,ltrim(ltrim(substring($"_c0",mapColumns(k)._1.toInt,mapColumns(k)._2.toInt)),"0"))
        }.drop($"_c0")

      case "json" =>
        val rawDf = sparkSession.read.option("multiline","true").json(s"src/main/resources/prueba/$fileName")
        val mapColumns = parameters.select($"name").collect().map(row=>row.getString(0))
        val dropColumns = mapColumns.filter(name => {
          if(name.contains(".")) {
            if(name.contains("`")){
              if(name.replaceAll("`.+`","").length>0) true
              else false
            }
            else true
          }
          else false
        })
        .map(n=>n.replaceAll("\\..+",""))
        .toSet.toList

        mapColumns.foldLeft(rawDf){
          (df,n) => df.withColumn(n,col(n))
        }.drop(dropColumns.head).drop(dropColumns.tail:_*)

      case "yaml" =>
        val filename = s"src/main/resources/prueba/$fileName"
        val yamlString = Source.fromFile(filename).getLines().toList.mkString("\n")
        val yamlReader = new ObjectMapper(new YAMLFactory)
        val obj = yamlReader.readValue(yamlString,classOf[Any])
        val jsonWriter = new ObjectMapper()
        val jsonString = jsonWriter.writeValueAsString(obj)

        val rawDf = sparkSession.read.json(Seq(jsonString).toDS())

        val mapColumns = parameters.select($"name").collect().map(row=>row.getString(0))
        val dropColumns = mapColumns.filter(name => {
          if(name.contains(".")) {
            if(name.contains("`")){
              if(name.replaceAll("`.+`","").length>0) true
              else false
            }
            else true
          }
          else false
        })
          .map(n=>n.replaceAll("\\..+",""))
          .toSet.toList

        dropColumns.foreach(x=>println(x))
        mapColumns.foldLeft(rawDf){
          (df,n) => df.withColumn(n,col(n))
        }.drop(dropColumns.head).drop(dropColumns.tail:_*)

    }

    println("Dynamic file reading with schema in provisioning table")
    df.printSchema()
    df.show()



    val periodos = sparkSession.createDataFrame(Seq(
      ("1","01-2021","01-01-2021","31-01-2021","ABIERTO")
      ,("1","02-2021","01-02-2021","28-02-2021","CERRADO")
      ,("1","03-2021","01-03-2021","31-03-2021","CERRADO")
      ,("1","04-2021","01-04-2021","30-04-2021","ABIERTO")
      ,("1","05-2021","01-05-2021","31-05-2021","CERRADO")
      ,("1","06-2021","01-06-2021","30-06-2021","ABIERTO")
      ,("2","05-2021","01-05-2021","31-05-2021","CERRADO")
      ,("2","06-2021","01-06-2021","30-06-2021","ABIERTO")
    )).toDF("sociedad","nombre_periodo","inicio","fin","estado")


    val data = sparkSession.createDataFrame(Seq(
      ("1","01-2021","05-01-2021","MENSUAL")
      ,("1","01-2021","07-01-2021","DIARIO")
      ,("1","02-2021","03-02-2021","MENSUAL")
      ,("1","03-2021","17-03-2021","MENSUAL")
      ,("2","05-2021","22-05-2021","DIARIO")
      ,("2","04-2021","10-04-2021","MENSUAL")
      ,("1","05-2021","10-04-2021","MENSUAL")
      ,("1","06-2021","02-06-2021","DIARIO")
    )).toDF("sociedad","nombre_periodo","fec_contab","tipo_periodo")


    val rawDates = periodos.filter($"estado".equalTo("ABIERTO"))
      .select($"sociedad",$"nombre_periodo",$"inicio",$"fin")
      .orderBy(to_date($"nombre_periodo","MM-yyyy").desc).collect().map(x=>List(x.getString(0),x.getString(1).substring(3,7).concat(x.getString(1).substring(0,2)),x.getString(2),x.getString(3)))


  println("Filter data with parametric table")
    rawDates.filter(x=>x(0).equals("1")).foreach(x=>println(x))


    def doUdf = (date:String,tipo:String,sociedad:String)=>{

      val dates = rawDates.filter(x=>x(0).equals(sociedad))

      val rawDate = dates.takeWhile(dateP => dateP(1).toInt > date.toInt).last

      if(tipo.toLowerCase().equals("mensual")) rawDate(3)
      else rawDate(2)
    }

    val getFirstOpen = udf(doUdf)

    data.join(periodos,Seq("sociedad","nombre_periodo"))
      .filter($"estado".equalTo("CERRADO"))
      .withColumn("fecha_bueno",getFirstOpen(concat(substring($"fec_contab",-4,4),substring($"fec_contab",4,2)),$"tipo_periodo",$"sociedad")).show()


    val ventana = Window.partitionBy(data("sociedad"), data("fec_contab"), data("tipo_periodo")).orderBy(periodos("nombre_periodo"))

    data.join(periodos.where(periodos("estado") === "ABIERTO"), periodos("sociedad") === data("sociedad") && periodos("nombre_periodo") >= data("nombre_periodo"),"left")
      .withColumn("rank", rank().over(ventana))
      .where(col("rank") === 1).drop("rank","estado")
      .withColumn("fecha_bueno"
        , when(data("tipo_periodo") === "MENSUAL", periodos("fin"))
          .when(data("nombre_periodo") === periodos("nombre_periodo"), data("fec_contab"))
          .otherwise(periodos("inicio"))).show()

    println("decimal(5,6)".substring(0,7))

    println(data.schema.map(field=>{
      field.name.concat(" ").concat(try{ if(field.dataType.typeName.substring(0,7).equals("decimal")) "decimal" else field.dataType.typeName} catch { case e:Exception => field.dataType.typeName})
    }).mkString(","))
  }

def refactorDate(rawDate: Column): Column = {
  val colDate = regexp_replace(rawDate,"-[0-9][0-9]$","")

  when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("01"),concat(lit("Enero "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("02"),concat(lit("Febrero "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("03"),concat(lit("Marzo "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("04"),concat(lit("Abril "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("05"),concat(lit("Mayo "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("06"),concat(lit("Junio "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("07"),concat(lit("Julio "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("08"),concat(lit("Agosto "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("09"),concat(lit("Septiembre "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("10"),concat(lit("Octubre "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .when(regexp_extract(colDate,"[0-9][0-9]$",0).equalTo("11"),concat(lit("Noviembre "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
    .otherwise(concat(lit("Diciembre "),regexp_extract(colDate,"^[0-9][0-9][0-9][0-9]",0)))
}

}
