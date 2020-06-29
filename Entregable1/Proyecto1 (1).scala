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

// Consultar etnias
data.groupBy("etnia").count().sort($"count".desc).show


// COMMAND ----------

// MAGIC %md
// MAGIC 1.Cuantos son los hombres y mujeres que hay en cada una de las 2 primeras etnias mas nombradas 

// COMMAND ----------

// Guardado de variable de la primera y segunda etnia que mas participa en el documento
val dataEtniaMas1 = data.where($"etnia" === "6 - Mestizo")
val dataEtniaMas2 = data.where($"etnia" === "1 - Indígena")

// COMMAND ----------

// Cuantos mestizos que son hombres hay
val dataH1 = dataEtniaMas1.where($"genero" === "1 - Hombre")
println(dataH1.count)

// COMMAND ----------

// Cuantos mestizos que son mujeres hay
val dataM1 = dataEtniaMas1.where($"genero" === "2 - Mujer")
println(dataM1.count)

// COMMAND ----------

// Cuantos indígenas que son hombres hay
val dataH2 = dataEtniaMas2.where($"genero" === "1 - Hombre")
println(dataH2.count)

// COMMAND ----------

// Cuantos indigenas que son mujeres hay
val dataM2 = dataEtniaMas2.where($"genero" === "2 - Mujer")
println(dataM2.count)

// COMMAND ----------

// MAGIC %md
// MAGIC 2.Porcentaje de hombres y mujeres en cada una de las 2 etnias mas nombradas

// COMMAND ----------

println(f"Mestizos\n${(dataH1.count / dataEtniaMas1.count.toDouble) * 100}%.2f%% Hombres\n${(dataM1.count / dataEtniaMas1.count.toDouble) * 100}%.2f%% Mujeres")

// COMMAND ----------

println(f"Indígenas\n${(dataH2.count / dataEtniaMas2.count.toDouble) * 100}%.2f%% Hombres\n${(dataM2.count / dataEtniaMas2.count.toDouble) * 100}%.2f%% Mujeres")

// COMMAND ----------

// MAGIC %md
// MAGIC 3.Cuantos hombres y mujeres hay en el area urbana de cada una de las 2 etnias

// COMMAND ----------

data.select("area").distinct().show

// COMMAND ----------

// Cauntos mestizos hombres hay en el área urbana
val dataAreaMesH1 = dataH1.where($"area" === "1 - Urbana")
println(dataAreaMesH1.count)

// COMMAND ----------

// Cauntos mestizos mujeres hay en el área urbana
val dataAreaMesM1 = dataM1.where($"area" === "1 - Urbana")
println(dataAreaMesM1.count)

// COMMAND ----------

// Cauntos indígenas hombres hay en el área urbana
val dataAreaIndiH1 = dataH2.where($"area" === "1 - Urbana")
println(dataAreaIndiH1.count)

// COMMAND ----------

// Cauntos indígenas mujeres hay en el área urbana
val dataAreaIndiM1 = dataM2.where($"area" === "1 - Urbana")
println(dataAreaIndiM1.count)

// COMMAND ----------

// MAGIC %md
// MAGIC 4.Cual es el valor promedio de ingreso que ganan los hombres y mujeres del area urbana de cada una de las 2 etnias mas nombradas

// COMMAND ----------

dataAreaMesH1.select(avg("ingreso_laboral").as("Promedio Hombres Mestizos")).show

// COMMAND ----------

dataAreaMesM1.select(avg("ingreso_laboral").as("Promedio Mujeres Mestizas")).show

// COMMAND ----------

dataAreaIndiH1.select(avg("ingreso_laboral").as("Promedio Hombres Indígenas")).show

// COMMAND ----------

dataAreaIndiM1.select(avg("ingreso_laboral").as("Promedio Mujeres Indígenas")).show

// COMMAND ----------

// MAGIC %md
// MAGIC 5.Cual es la ocupación mas nombrada entre los hombres y mujeres de las 2 etnias en el area urbana

// COMMAND ----------

dataAreaMesH1.groupBy($"grupo_ocupacion".as("Ocupacion de Hombres mestizos")).count().sort($"count".desc).show(false)

// COMMAND ----------

dataAreaMesM1.groupBy($"grupo_ocupacion".as("Ocupacion de Mujeres mestizas")).count().sort($"count".desc).show(false)

// COMMAND ----------

dataAreaIndiH1.groupBy($"grupo_ocupacion".as("Ocupacion de Hombres indígenas")).count().sort($"count".desc).show(false)

// COMMAND ----------

dataAreaIndiM1.groupBy($"grupo_ocupacion".as("Ocupacion de Mujeres indígenas")).count().sort($"count".desc).show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 6.Cual es ingreso mínino y máximo que ganan los hombres y mujeres de cada etnia seleccionada en el area urbana

// COMMAND ----------

println("Hombres Mestizos")
dataAreaMesH1.select(min("ingreso_laboral").as("Ingreso mínimo"), max("ingreso_laboral").as("Ingreso máximo")).show

// COMMAND ----------

println("Mujeres Mestizas")
dataAreaMesM1.select(min("ingreso_laboral").as("Ingreso mínimo"), max("ingreso_laboral").as("Ingreso máximo")).show

// COMMAND ----------

println("Hombres Indígenas")
dataAreaIndiH1.select(min("ingreso_laboral").as("Ingreso mínimo"), max("ingreso_laboral").as("Ingreso máximo")).show

// COMMAND ----------

println("Mujeres Indígenas")
dataAreaIndiM1.select(min("ingreso_laboral").as("Ingreso mínimo"), max("ingreso_laboral").as("Ingreso máximo")).show

// COMMAND ----------

// MAGIC %md
// MAGIC 7.En que ocupación gana el máximo el hombre y la mujer de las 2 etnias

// COMMAND ----------

println("Hombre Mestizo")
dataAreaMesH1.select("grupo_ocupacion").where($"ingreso_laboral" === "146030").show(false)

// COMMAND ----------

println("Mujer Mestiza")
dataAreaMesM1.select("grupo_ocupacion").where($"ingreso_laboral" === "70000").show(false)

// COMMAND ----------

println("Hombre Indígena")
dataAreaIndiH1.select("grupo_ocupacion").where($"ingreso_laboral" === "44234").show(false)

// COMMAND ----------

println("Mujer Indígena")
dataAreaIndiM1.select("grupo_ocupacion").where($"ingreso_laboral" === "10000").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 8.En que rama de actividad se encuentra ganando el ingreso máximo lo hombres y mujeres de las 2 etnias

// COMMAND ----------

println("Hombres Mestizos")
dataAreaMesH1.select("rama_actividad").where($"grupo_ocupacion" === "01 - Personal direct./admin. pública y empresas" && $"ingreso_laboral" === "146030").show(false)

// COMMAND ----------

println("Mujeres Mestizas")
dataAreaMesM1.select("rama_actividad").where($"grupo_ocupacion" === "01 - Personal direct./admin. pública y empresas" && $"ingreso_laboral" === "70000").show(false)

// COMMAND ----------

println("Hombre Indígena")
dataAreaIndiH1.select("rama_actividad").where($"grupo_ocupacion" === "05 - Trabajad. de los servicios y comerciantes" && $"ingreso_laboral" === "44234").show(false)

// COMMAND ----------

println("Mujer Indígena")
dataAreaIndiM1.select("rama_actividad").where($"grupo_ocupacion" === "05 - Trabajad. de los servicios y comerciantes" && $"ingreso_laboral" === "10000").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC 9.Cuantos mestizos e indígenas ganan por encima de la media

// COMMAND ----------

dataEtniaMas1.select(mean("ingreso_laboral").as("Media de Mestizos")).show

// COMMAND ----------

val dataMesMedia = dataEtniaMas1.where($"ingreso_laboral" >= "516.6195378181872")
println(dataMesMedia.count)

// COMMAND ----------

dataEtniaMas2.select(mean("ingreso_laboral").as("Media de Indígenas")).show

// COMMAND ----------

val dataIndiMedia = dataEtniaMas1.where($"ingreso_laboral" >= "297.09649890123956")
println(dataIndiMedia.count)

// COMMAND ----------

// MAGIC %md
// MAGIC 10.De acuerdo a la pregunta anterior que porcentaje son hombres y mujeres

// COMMAND ----------

val dataMesMediaH = dataMesMedia.where($"genero" === "1 - Hombre")
val dataMesMediaM = dataMesMedia.where($"genero" === "2 - Mujer")

// COMMAND ----------

println(f"${(dataMesMediaH.count / dataMesMedia.count.toDouble) * 100}%.2f%% Hombres\n${(dataMesMediaM.count / dataMesMedia.count.toDouble) * 100}%.2f%% Mujeres")

// COMMAND ----------

val dataIndiMediaH = dataIndiMedia.where($"genero" === "1 - Hombre")
val dataIndiMediaM = dataIndiMedia.where($"genero" === "2 - Mujer")

// COMMAND ----------

println(f"${(dataIndiMediaH.count / dataIndiMedia.count.toDouble) * 100}%.2f%% Hombres\n${(dataIndiMediaM.count / dataIndiMedia.count.toDouble) * 100}%.2f%% Mujeres")
