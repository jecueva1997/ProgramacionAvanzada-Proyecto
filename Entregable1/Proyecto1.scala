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
