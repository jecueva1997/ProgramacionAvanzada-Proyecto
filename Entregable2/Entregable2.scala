// Databricks notebook source
// Lectura de datos
import org.apache.spark.sql.types._
val myDataSchema = StructType(
    Array(
        StructField("id_persona", DecimalType(26, 0), true), 
        StructField("anio", IntegerType, true), 
        StructField("mes", IntegerType, true), 
        StructField("provincia", IntegerType, true), 
        StructField("canton", IntegerType, true), 
        StructField("area", StringType, true), 
        StructField("genero", StringType, true), 
        StructField("edad", IntegerType, true), 
        StructField("estado_civil", StringType, true), 
        StructField("nivel_de_instruccion", StringType, true), 
        StructField("etnia", StringType, true), 
        StructField("ingreso_laboral", IntegerType, true), 
        StructField("condicion_actividad", StringType, true), 
        StructField("sectorizacion", StringType, true), 
        StructField("grupo_ocupacion", StringType, true), 
        StructField("rama_actividad", StringType, true), 
        StructField("factor_expansion", DoubleType, true)
    ));

// COMMAND ----------

// Creacion del data
val data = spark
  .read
  .schema(myDataSchema)
//  .option("inferSchema", true)
  .option("header", "true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

//Saber la cantidad de filas que hay
println($"TOTAL DE DATOS = ${data.count}")

// COMMAND ----------

// MAGIC %md
// MAGIC Saber si hay nulos en los datos, con referencia al ingreso_laboral

// COMMAND ----------

data.select("ingreso_laboral").groupBy("ingreso_laboral").count().sort($"count".desc).show(4)

// COMMAND ----------

// MAGIC %md
// MAGIC Eliminamos los datos nulos en referencia de la columna ingeso_laboral

// COMMAND ----------

val newData = data.select("id_persona", "anio", "area", "genero", "etnia", "ingreso_laboral", "grupo_ocupacion", "rama_actividad").where($"ingreso_laboral".isNotNull)
println(newData.count)

// COMMAND ----------

// hacemos un summary al ingreso_laboral para saber la cantidad de datos q hay y cual es el maximo
newData.select("ingreso_laboral").summary().show()

// COMMAND ----------

// MAGIC %md
// MAGIC Dividimos los datos en rangos para saber cuales son los que mas hay

// COMMAND ----------

val cantValoresRangos = scala.collection.mutable.ListBuffer[Long]()
val minValor = 0.0
val maxValor = 146030
val bins = 5.0
val rango = (maxValor - minValor) / bins
var minCount = minValor
var maxCount = rango
while (minCount < maxValor) {
  val valoresRango = newData.where($"ingreso_laboral".between(minCount, maxCount))
  cantValoresRangos.+=(valoresRango.count())
  minCount = maxCount
  maxCount += rango
}
println("Valores en diferentes rangos: ")
cantValoresRangos.foreach(println)

// COMMAND ----------

// importamos las funciones para poder realizar calculos como la media y la desviacion estandar
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos la media

// COMMAND ----------

val avg = newData.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos la desviacion estandar

// COMMAND ----------

val stdDev = newData.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos los limites

// COMMAND ----------

val limInferior = avg - 3 * stdDev
val limSuperior = avg + 3 * stdDev

// COMMAND ----------

// MAGIC %md
// MAGIC Filtramos los datos

// COMMAND ----------

// filtro de datos menos al limInferior
val valInferiorAllimInferior = newData.where($"ingreso_laboral" < limInferior)
valInferiorAllimInferior.describe().show

// COMMAND ----------

// filtro de datos mayores al limSuperior
val valMayorAllimSuperior = newData.where($"ingreso_laboral" > limSuperior)
valMayorAllimSuperior.describe().show

// COMMAND ----------

// se muestra los datos ordenados descendentemente
valMayorAllimSuperior.orderBy($"ingreso_laboral".desc).show(504742, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Data sin Outliers

// COMMAND ----------

val newDataSinOutliers = newData.where($"ingreso_laboral" > limInferior && $"ingreso_laboral" < limSuperior)

// COMMAND ----------

// realizamos un summary para saber la cantidad de datos y cual es el maximo
newDataSinOutliers.select("ingreso_laboral").summary().show

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que ya no hay datos outliers se procede a realizar lo del entregable

// COMMAND ----------

// Consultar etnias
newDataSinOutliers.groupBy("etnia").count().sort($"count".desc).show

// COMMAND ----------

// MAGIC %md
// MAGIC 1.Cuantos son los hombres y mujeres que hay en cada una de las 2 primeras etnias mas nombradas en cada año

// COMMAND ----------

// Guardado de variable de la primera y segunda etnia que mas participa en el documento
val dataEtniaMas1 = newDataSinOutliers.where($"etnia" === "6 - Mestizo")
val dataEtniaMas2 = newDataSinOutliers.where($"etnia" === "1 - Indígena")

// COMMAND ----------

val dataGeneroMestizos = dataEtniaMas1.groupBy("anio").pivot("genero").count().orderBy("anio")
val dataGeneroIndigenas = dataEtniaMas2.groupBy("anio").pivot("genero").count().orderBy("anio")

println("Mestizos")
dataGeneroMestizos.show
println("Indígenas")
dataGeneroIndigenas.show

// COMMAND ----------

// DBTITLE 1,Gráfica de Mestizos
display(dataGeneroMestizos)

// COMMAND ----------

// DBTITLE 1,Gráfica de Indígenas
display(dataGeneroIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 2.Cuantos hombres y mujeres hay en el area urbana y rural de cada una de las 2 etnias

// COMMAND ----------

// consulta de las areas que hay
data.select("area").distinct().show

// COMMAND ----------

val dataAreaMestizos = dataEtniaMas1.groupBy("area").pivot("genero").count().orderBy("area")
val dataAreaIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").count().orderBy("area")

println("Mestizos")
dataAreaMestizos.show
println("Indígenas")
dataAreaIndigenas.show

// COMMAND ----------

// DBTITLE 1,Gráfica de Mestizos en cuanto a las Áreas
display(dataAreaMestizos)

// COMMAND ----------

// DBTITLE 1,Gráfica de Indígenas en cuanto a las Áreas
display(dataAreaIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 3.Cual es el valor promedio de ingreso que ganan los hombres y mujeres del area urbana y rural de cada una de las 2 etnias mas nombradas

// COMMAND ----------

val dataIngresoAvgMestizos = dataEtniaMas1.groupBy("area").pivot("genero").avg("ingreso_laboral").orderBy("area")
val dataIngresoAvgIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").avg("ingreso_laboral").orderBy("area")

println("Mestizos")
dataIngresoAvgMestizos.show
println("Indígenas")
dataIngresoAvgIndigenas.show

// COMMAND ----------

// DBTITLE 1,Gráfica del promedio de ingresos de Mestizos hombres y mujeres de cada área
display(dataIngresoAvgMestizos)

// COMMAND ----------

// DBTITLE 1,Gráfica del promedio de ingresos de Indígenas hombres y mujeres de cada área
display(dataIngresoAvgIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 4.Cual es la ocupación mas nombrada entre los hombres y mujeres de las 2 etnias en el area urbana y rural

// COMMAND ----------

// DBTITLE 1,Ocupación mestizos
val dataMestizosU_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataMestizosU_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataMestizosR_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataMestizosR_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

val dataOcupacionMestizosU_H = dataMestizosU_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Urbana")).count().sort($"count".desc)
val dataOcupacionMestizosU_M = dataMestizosU_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Urbana")).count().sort($"count".desc)
val dataOcupacionMestizosR_H = dataMestizosR_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Rural")).count().sort($"count".desc)
val dataOcupacionMestizosR_M = dataMestizosR_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Rural")).count().sort($"count".desc)

dataOcupacionMestizosU_H.show(false)
dataOcupacionMestizosU_M.show(false)
dataOcupacionMestizosR_H.show(false)
dataOcupacionMestizosR_M.show(false)

// COMMAND ----------

// DBTITLE 1,Ocupación Indígenas
val dataIndigenasU_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataIndigenasU_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataIndigenasR_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataIndigenasR_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

val dataOcupacionIndigenasU_H = dataIndigenasU_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Urbana")).count().sort($"count".desc)
val dataOcupacionIndigenasU_M = dataIndigenasU_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Urbana")).count().sort($"count".desc)
val dataOcupacionIndigenasR_H = dataIndigenasR_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Rural")).count().sort($"count".desc)
val dataOcupacionIndigenasR_M = dataIndigenasR_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Rural")).count().sort($"count".desc)

dataOcupacionIndigenasU_H.show(false)
dataOcupacionIndigenasU_M.show(false)
dataOcupacionIndigenasR_H.show(false)
dataOcupacionIndigenasR_M.show(false)
