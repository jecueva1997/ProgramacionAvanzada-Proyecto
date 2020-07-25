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

// MAGIC %md
// MAGIC Hacemos un summary al ingreso_laboral para saber la cantidad de datos q hay y cual es el maximo

// COMMAND ----------


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

// DBTITLE 1,Data sin Outliers
// MAGIC %md
// MAGIC Es una observación que es numéricamente distante del resto de los datos

// COMMAND ----------

val newDataSinOutliers = newData.where($"ingreso_laboral" > limInferior && $"ingreso_laboral" < limSuperior)

// COMMAND ----------

// MAGIC %md
// MAGIC Realizamos un summary para saber la cantidad de datos y cual es el maximo

// COMMAND ----------

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

// MAGIC %md
// MAGIC Se procede Guardado de variable de la primera y segunda etnia que mas participa en el documento

// COMMAND ----------

val dataEtniaMas1 = newDataSinOutliers.where($"etnia" === "6 - Mestizo")
val dataEtniaMas2 = newDataSinOutliers.where($"etnia" === "1 - Indígena")

// COMMAND ----------

// MAGIC %md
// MAGIC En las variables ya mencionadas se hace un pivote de la variable género donde tomara los únicos valores que poseea esta columna, además se lo agrupara por año para saber por los distintos años en donde se encuentra la mayor cantidad y se lo ordenara por esta mismo para una mejor visualización en tablas para cada una de las dos datas 

// COMMAND ----------

val dataGeneroMestizos = dataEtniaMas1.groupBy("anio").pivot("genero").count().orderBy("anio")
val dataGeneroIndigenas = dataEtniaMas2.groupBy("anio").pivot("genero").count().orderBy("anio")

println("Mestizos")
dataGeneroMestizos.show
println("Indígenas")
dataGeneroIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC En la anterior consulta se lo pudo ver en lo que son tablas ahora se lo presentará por gráficas con la palabra reservada display

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

// MAGIC %md
// MAGIC Para poder sacar la siguiente pregunta se realiza la consulta de las areas que hay para poder responder

// COMMAND ----------

data.select("area").distinct().show

// COMMAND ----------

// MAGIC %md
// MAGIC Tomando en cuenta con las 2 primeras datas en donde se tiene ya las 2 etnias mas guardadas se procede a realizar un pivo de género en donde con sus datos únicos se agrupara por área para mostrar en donde existen la mayor población 

// COMMAND ----------

val dataAreaMestizos = dataEtniaMas1.groupBy("area").pivot("genero").count().orderBy("area")
val dataAreaIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").count().orderBy("area")

println("Mestizos")
dataAreaMestizos.show
println("Indígenas")
dataAreaIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC Con la anterior consulta ya presentada en tablas se la realiza de la misma manera por gráficas

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

// MAGIC %md
// MAGIC Trabajando con las 2 datas de las etnias se procede a hacer un pivote de la columana género y para poder responder la pregunta ya mencionada se saca el valor promedio de la variable ingreso_laboral y como se desea saber el area se la agrupa por esta mismo

// COMMAND ----------

val dataIngresoAvgMestizos = dataEtniaMas1.groupBy("area").pivot("genero").avg("ingreso_laboral").orderBy("area")
val dataIngresoAvgIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").avg("ingreso_laboral").orderBy("area")

println("Mestizos")
dataIngresoAvgMestizos.show
println("Indígenas")
dataIngresoAvgIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC Con los valores ya mencionados se realiza una gráfica de los mismos

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

// MAGIC %md
// MAGIC Para la siguiente consulta por cada data ya guardada de la etnia mestizo se procede a hacer un .where de esta misma data en donde por hombres y mujeres que se encuentren en la variable género se hace una igualdad por cada una de las 2 áreas únicas ya existentes en la misma

// COMMAND ----------

// DBTITLE 1,Ocupación mestizos
val dataMestizosU_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataMestizosU_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataMestizosR_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataMestizosR_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

// MAGIC %md
// MAGIC Ya con la data obtenida de cada uno  de los hombres y mujeres de la etnia mestizos y por su área, se procede a sacar por cada una de las ya mencionadas el grupo_ocupacion y a realizar un conteo para poder ver en donde se encunetra una mayor afluencia de registros

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

// MAGIC %md
// MAGIC Para la siguiente consulta por cada data ya guardada de la etnia indígena se procede a hacer un .where de esta misma data en donde por hombres y mujeres que se encuentren en la variable género se hace una igualdad por cada una de las 2 áreas únicas ya existentes en la misma

// COMMAND ----------

// DBTITLE 1,Ocupación Indígenas
val dataIndigenasU_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataIndigenasU_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataIndigenasR_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataIndigenasR_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

// MAGIC %md
// MAGIC Ya con la data obtenida de cada uno  de los hombres y mujeres de la etnia indígenas y por su área, se procede a sacar por cada una de las ya mencionadas el grupo_ocupacion y a realizar un conteo para poder ver en donde se encunetra una mayor afluencia de registros

// COMMAND ----------

val dataOcupacionIndigenasU_H = dataIndigenasU_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Urbana")).count().sort($"count".desc)
val dataOcupacionIndigenasU_M = dataIndigenasU_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Urbana")).count().sort($"count".desc)
val dataOcupacionIndigenasR_H = dataIndigenasR_H.groupBy($"grupo_ocupacion".as("Ocupacion Hombres area Rural")).count().sort($"count".desc)
val dataOcupacionIndigenasR_M = dataIndigenasR_M.groupBy($"grupo_ocupacion".as("Ocupacion Mujeres area Rural")).count().sort($"count".desc)

dataOcupacionIndigenasU_H.show(false)
dataOcupacionIndigenasU_M.show(false)
dataOcupacionIndigenasR_H.show(false)
dataOcupacionIndigenasR_M.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 5.Cual es ingreso máximo que ganan los hombres y mujeres de cada etnia seleccionada en el area urbana y rural

// COMMAND ----------

// DBTITLE 1,Ingreso Max de Mestizos
val dataIngresoMaxMestizos = dataEtniaMas1.groupBy("area").pivot("genero").max("ingreso_laboral").orderBy("area")
dataIngresoMaxMestizos.show

// COMMAND ----------

display(dataIngresoMaxMestizos)

// COMMAND ----------

// DBTITLE 1,Ingreso Max de Indígenas
val dataIngresoMaxIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").max("ingreso_laboral").orderBy("area")
dataIngresoMaxIndigenas.show

// COMMAND ----------

display(dataIngresoMaxIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 6.En que ocupación gana el máximo el hombre y la mujer de las 2 etnias en cada area

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
dataMestizosU_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2784").distinct.show(false)
dataMestizosU_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2790").distinct.show(false)
dataMestizosR_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2783").distinct.show(false)
dataMestizosR_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2766").distinct.show(false)

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
dataIndigenasU_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2764").distinct.show(false)
dataIndigenasU_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2780").distinct.show(false)
dataIndigenasR_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2790").distinct.show(false)
dataIndigenasR_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2741").distinct.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 7.En que rama de actividad se encuentra ganando el ingreso máximo lo hombres y mujeres de las 2 etnias en cada area

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
dataMestizosU_H.select($"rama_actividad".as("Actividad de Hombres del area Urbana")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2784").show(false)

dataMestizosU_M.select($"rama_actividad".as("Actividad de Mujeres del area Urbana")).where(($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" || $"grupo_ocupacion" === "03 - Técnicos y profesionales de nivel medio") && $"ingreso_laboral" === "2790").show(false)

dataMestizosR_H.select($"rama_actividad".as("Actividad de Hombres del area Rural")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2783").show(false)

dataMestizosR_M.select($"rama_actividad".as("Actividad de Mujeres del area Rural")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2766").show(false)

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
dataIndigenasU_H.select($"rama_actividad".as("Actividad de Hombres del area Urbana")).where($"grupo_ocupacion" === "05 - Trabajad. de los servicios y comerciantes" && $"ingreso_laboral" === "2764").show(false)

dataIndigenasU_M.select($"rama_actividad".as("Actividad de Mujeres del area Urbana")).where($"grupo_ocupacion" === "05 - Trabajad. de los servicios y comerciantes" && $"ingreso_laboral" === "2780").show(false)

dataIndigenasR_H.select($"rama_actividad".as("Actividad de Hombres del area Rural")).where($"grupo_ocupacion" === "01 - Personal direct./admin. pública y empresas" && $"ingreso_laboral" === "2790").show(false)

dataIndigenasR_M.select($"rama_actividad".as("Actividad de Mujeres del area Rural")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2741").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 8.Cuantos mestizos e indígenas ganan por encima de la media

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
// sacamos la media de todos los datos de mestizos sin outliers
dataEtniaMas1.select(mean("ingreso_laboral").as("Media de Mestizos")).show

// COMMAND ----------

// mestizos que ganan por encima de la media
val dataMesMedia = dataEtniaMas1.where($"ingreso_laboral" >= "516.6195378181872")
println(dataMesMedia.count)

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
// sacamos la media de todos los datos de indigenas sin outliers
dataEtniaMas2.select(mean("ingreso_laboral").as("Media de Indígenas")).show

// COMMAND ----------

// indigenas que ganan por encima de la media
val dataIndiMedia = dataEtniaMas1.where($"ingreso_laboral" >= "297.09649890123956")
println(dataIndiMedia.count)

// COMMAND ----------

// MAGIC %md
// MAGIC 9.De acuerdo a la pregunta anterior cuantos son hombres y mujeres. Muestre por cada año que se hizo la encuesta

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
val dataMediaF1 = dataMesMedia.groupBy("anio").pivot("genero").count().orderBy("anio")
dataMediaF1.show

// COMMAND ----------

display(dataMediaF1)

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
val dataMediaF2 = dataIndiMedia.groupBy("anio").pivot("genero").count().orderBy("anio")
dataMediaF2.show

// COMMAND ----------

display(dataMediaF2)
