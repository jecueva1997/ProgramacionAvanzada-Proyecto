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

// MAGIC %md
// MAGIC realizamos un .count al data para saber la catidad de filas que existen en el csv

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
// MAGIC Dividimos los datos en rangos para saber cuales son los que mas hay.
// MAGIC 
// MAGIC La variable minValor es igual a cero y maxvalor es igual a 146030 puesto que en el summary anterior se pudo observar cual es el minimo y cual es el maximo valor que hay en nuestro data con referencia al ingreso_laboral. Y para terminar dentro del while vamos contando cuantos datos dentro de los rangos (la variable bins contiene la cantidad de rangos que vamos a tener) existen y luego los presentamos.

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
// MAGIC Calculamos los limites.
// MAGIC 
// MAGIC Con la ayuda de la media y la desviación estandar calculamos los limites de nuestros valores de ingreso_laboral, en donde primeramente nuestro limInferior se puede observar que tiene un valor negativo y limSuperior un valor positivo, lo cual quiere decir que nuestros datos sin outliers deben estar entre estos rangos, y los demas datos ya sean menores al limInferior o mayores al limSuperior son los datos que se encuentran por asi decirlo de manera excesiva.

// COMMAND ----------

val limInferior = avg - 3 * stdDev
val limSuperior = avg + 3 * stdDev

// COMMAND ----------

// MAGIC %md
// MAGIC Filtramos los datos

// COMMAND ----------

// MAGIC %md
// MAGIC Este filtro se hace solo con la intención de saber si existen datos por debajo del limInferior. Pero como se ve en los resultados no hay ninguno puesto que no hay un ingreso laboral que sea negativo.

// COMMAND ----------

// filtro de datos menos al limInferior
val valInferiorAllimInferior = newData.where($"ingreso_laboral" < limInferior)
valInferiorAllimInferior.describe().show

// COMMAND ----------

// MAGIC %md
// MAGIC En este filtro se buscan los datos que sean mayores al limSuperior. Aqui hay que tener algo en cuenta y es que estos datos al ser ya mayores al limSuperior se consideran excesivos por lo que ya se consideran datos outliers.

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
// MAGIC A continuación guardamos solamente los datos que se encuentran entre estos limites ya mencionados anteriormente, ya que los que no se encuentran entre estos se consideran datos outliers y se quitarian de nuestra data.

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
// MAGIC Como ya sabemos de que etnias hay mas datos gracias a la consulta anterior realizada, se procede a guardar en un data diferente para cada etnia toda la información que existe en nuestro dataSinOutliers que les corresponda a cada una.

// COMMAND ----------

val dataEtniaMas1 = newDataSinOutliers.where($"etnia" === "6 - Mestizo")
val dataEtniaMas2 = newDataSinOutliers.where($"etnia" === "1 - Indígena")

// COMMAND ----------

// MAGIC %md
// MAGIC En las variables ya mencionadas se hace un pivote de la variable género donde tomara los únicos valores que poseea esta columna, además se lo agrupara por año para saber por los distintos años en donde se encuentra la mayor cantidad de hombres y mujeres que han respondido la encuesta que se haya realizado para obtener este data y se lo ordenara por esta mismo para una mejor visualización en tablas para cada una de las dos datas 

// COMMAND ----------

val dataGeneroMestizos = dataEtniaMas1.groupBy("anio").pivot("genero").count().orderBy("anio")
val dataGeneroIndigenas = dataEtniaMas2.groupBy("anio").pivot("genero").count().orderBy("anio")

println("Mestizos")
dataGeneroMestizos.show
println("Indígenas")
dataGeneroIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC En la anterior consulta se lo pudo ver en lo que son tablas ahora se lo presentará por gráficas con la palabra reservada display, con la intencion de saber de manera mas exacta en que año hubo mas hombres y en cual mas mujeres, puesto que la anterior se ordenaba por el año y no se sabia en donde se habia encuestado mas hombres que mujeres o viceversa.

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
// MAGIC Tomando en cuenta las 2 primeras datas en donde se tiene ya las 2 etnias mas mencionadas se procede a realizar un pivot de género con la intencion de saber en donde hay mas hombres y mujeres.

// COMMAND ----------

val dataAreaMestizos = dataEtniaMas1.groupBy("area").pivot("genero").count().orderBy("area")
val dataAreaIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").count().orderBy("area")

println("Mestizos")
dataAreaMestizos.show
println("Indígenas")
dataAreaIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC Con la anterior consulta ya presentada en tablas, ahora se procede a mostrar por gráficas para tener una mejor visualizacion de los datos obtenidos.

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
// MAGIC Con los valores ya mencionados se realiza una gráfica de los mismos para mejor visualizacion de los datos obtenidos.

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
// MAGIC En esta parte se procede a guardar para la etnia mestizos en cuatro datas diferentes a los hombres y mujeres que sean del area urbana y rural por separado y para esto nos ayudamos del .where en donde le especificamos de que datos nomas queremos que guarde en cada data nuevo.

// COMMAND ----------

// DBTITLE 1,Ocupación mestizos
val dataMestizosU_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataMestizosU_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataMestizosR_H = dataEtniaMas1.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataMestizosR_M = dataEtniaMas1.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

// MAGIC %md
// MAGIC Ya con la data obtenida de cada uno  de los hombres y mujeres de la etnia mestizos y por su área, se procede a sacar por cada una de las ya mencionadas el grupo_ocupacion y a realizar un conteo para poder ver en donde se encunetra una mayor abundancia de registros.

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
// MAGIC En esta parte se procede a guardar para la etnia indigena en cuatro datas diferentes a los hombres y mujeres que sean del area urbana y rural por separado y para esto nos ayudamos del .where en donde le especificamos de que datos nomas queremos que guarde en cada data nuevo. Es decir se hace lo mismo que se hizo para los de la etnia mestizos.

// COMMAND ----------

// DBTITLE 1,Ocupación Indígenas
val dataIndigenasU_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "1 - Urbana")
val dataIndigenasU_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "1 - Urbana")
val dataIndigenasR_H = dataEtniaMas2.where($"genero" === "1 - Hombre" && $"area" === "2 - Rural")
val dataIndigenasR_M = dataEtniaMas2.where($"genero" === "2 - Mujer" && $"area" === "2 - Rural")

// COMMAND ----------

// MAGIC %md
// MAGIC Ya con la data obtenida de cada uno  de los hombres y mujeres de la etnia indígenas y por su área, se procede a sacar por cada una de las ya mencionadas el grupo_ocupacion y a realizar un conteo para poder ver en donde se encunetra una mayor afluencia de registros.

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

// MAGIC %md
// MAGIC De la data que contiene la información de la etnia mestizos, se realiza un pivot de la variable o columna genero en donde se agrupa y ordena por area y que mostrara como el resultado de la consulta el ingreso maximo que ganan los hombres y mujeres de cada area. No se saca el minimo del ingreso ya que como observamos en una de las consultas anteriores este tiene un valor de cero.

// COMMAND ----------

// DBTITLE 1,Ingreso Max de Mestizos
val dataIngresoMaxMestizos = dataEtniaMas1.groupBy("area").pivot("genero").max("ingreso_laboral").orderBy("area")
dataIngresoMaxMestizos.show

// COMMAND ----------

// MAGIC %md
// MAGIC A continuación se procede a mostrar de manera gráfica la consulta con la intención de visualizar de forma mas exacta quien tiene el mayor ingreso en cada una de las areas.

// COMMAND ----------

display(dataIngresoMaxMestizos)

// COMMAND ----------

// MAGIC %md
// MAGIC Al igual que la consulta anterior, de la data que contiene la información de la etnia indígena, se realiza un pivot de la variable o columna genero en donde se agrupa y ordena por area y que mostrara como el resultado de la consulta el ingreso maximo que ganan los hombres y mujeres de cada area. No se saca el minimo del ingreso ya que como observamos en una de las consultas anteriores este tiene un valor de cero.

// COMMAND ----------

// DBTITLE 1,Ingreso Max de Indígenas
val dataIngresoMaxIndigenas = dataEtniaMas2.groupBy("area").pivot("genero").max("ingreso_laboral").orderBy("area")
dataIngresoMaxIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC Se procede a mostrar de manera gráfica la consulta con la intención de visualizar de forma mas exacta quien tiene el mayor ingreso en cada una de las areas de la etnia indígena.

// COMMAND ----------

display(dataIngresoMaxIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 6.En que ocupación gana el máximo el hombre y la mujer de las 2 etnias en cada area

// COMMAND ----------

// MAGIC %md
// MAGIC Con la ayuda de la consulta de la pregunta cinco con respecto a la etnia mestizos y con los data obtenidos en la pregunta cuatro se procede a mostrar en forma de tabla en que ocupación ganan el maximo los hombres y mujeres de cada area de la etnia mestizos, y para esto nos ayudamos con el .where en donde le damos los detalles de los datos que queremos.

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
dataMestizosU_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2784").distinct.show(false)
dataMestizosU_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Urbana")).where($"ingreso_laboral" === "2790").distinct.show(false)
dataMestizosR_H.select($"grupo_ocupacion".as("Hombres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2783").distinct.show(false)
dataMestizosR_M.select($"grupo_ocupacion".as("Mujeres con Ingreso max del area Rural")).where($"ingreso_laboral" === "2766").distinct.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Con la ayuda de la consulta de la pregunta cinco con respecto a la etnia indígena y con los data obtenidos en la pregunta cuatro se procede a mostrar en forma de tabla en que ocupación ganan el maximo los hombres y mujeres de cada area de la etnia indígena, y para esto nos ayudamos con el .where en donde le damos los detalles de los datos que queremos.

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

// MAGIC %md
// MAGIC Ahora que ya sabemos en que ocupación estan ganando el maximo los hobres y mujeres de cada area de la etnia mestizos. Se procede a consultar la rama de actividad en donde se encuentran ganando ese ingreso, y aqui de la misma forma que la antrior nos ayudamos del .where al cual ya le añadimos la ocupación aparte del ingreso maximo.

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
dataMestizosU_H.select($"rama_actividad".as("Actividad de Hombres del area Urbana")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2784").show(false)

dataMestizosU_M.select($"rama_actividad".as("Actividad de Mujeres del area Urbana")).where(($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" || $"grupo_ocupacion" === "03 - Técnicos y profesionales de nivel medio") && $"ingreso_laboral" === "2790").show(false)

dataMestizosR_H.select($"rama_actividad".as("Actividad de Hombres del area Rural")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2783").show(false)

dataMestizosR_M.select($"rama_actividad".as("Actividad de Mujeres del area Rural")).where($"grupo_ocupacion" === "02 - Profesionales científicos e intelectuales" && $"ingreso_laboral" === "2766").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que ya sabemos en que ocupación estan ganando el maximo los hobres y mujeres de cada area de la etnia indigena. Se procede a consultar la rama de actividad en donde se encuentran ganando ese ingreso, y aqui de la misma forma que la antrior pregunta nos ayudamos del .where al cual ya le añadimos la ocupación aparte del ingreso maximo.

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

// MAGIC %md
// MAGIC Se realiza una pequeña consulta en donde tenemos como resultado el promedio o la media que ganan las personas de la etnia mestizos.

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
// sacamos la media de todos los datos de mestizos sin outliers
dataEtniaMas1.select(mean("ingreso_laboral").as("Media de Mestizos")).show

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que ya sabemos cual es la media, se procede a realizar un .count pra contar a todas las personas que estan ganando por encima de la media en la etnia mestizos, y para esto en el .where le damos la condicion de que solo queremos que cuente a aquellos con un ingreso mayor o igual a la media.

// COMMAND ----------

// mestizos que ganan por encima de la media
val dataMesMedia = dataEtniaMas1.where($"ingreso_laboral" >= "516.6195378181872")
println(dataMesMedia.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Se realiza una pequeña consulta en donde tenemos como resultado el promedio o la media que ganan las personas de la etnia Indígena.

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
// sacamos la media de todos los datos de indigenas sin outliers
dataEtniaMas2.select(mean("ingreso_laboral").as("Media de Indígenas")).show

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que ya sabemos cual es la media, se procede a realizar un .count pra contar a todas las personas que estan ganando por encima de la media en la etnia indígena, y para esto en el .where le damos la condicion de que solo queremos que cuente a aquellos con un ingreso mayor o igual a la media.

// COMMAND ----------

// indigenas que ganan por encima de la media
val dataIndiMedia = dataEtniaMas1.where($"ingreso_laboral" >= "297.09649890123956")
println(dataIndiMedia.count)

// COMMAND ----------

// MAGIC %md
// MAGIC 9.De acuerdo a la pregunta anterior cuantos son hombres y mujeres. Muestre por cada año que se hizo la encuesta

// COMMAND ----------

// MAGIC %md
// MAGIC Como se puede observar en la pregunta anterior se guardo en un data a aquellos de la etnia mestizos que ganan por encia de la media. Y ya teniendo eso en cuenta se procede a realizar un pivot de genero en ese data, en donde se agrupa y ordena por año con la intencion de saber cuantos hombres y mujeres por cada año ganaron por encima de la media.

// COMMAND ----------

// DBTITLE 1,Etnia Mestizos
val dataMediaF1 = dataMesMedia.groupBy("anio").pivot("genero").count().orderBy("anio")
dataMediaF1.show

// COMMAND ----------

// MAGIC %md
// MAGIC Para mejor visualización de los datos que se saco de la etnia mestizos anteriormente con el pivot, se muestra en forma de gráfica los datos para saber y poder consultar de forma mas exacta en donde hay mas hombres o mujeres que ganaron por encima de la media.

// COMMAND ----------

display(dataMediaF1)

// COMMAND ----------

// MAGIC %md
// MAGIC Como se puede observar en la pregunta anterior se guardo en un data a aquellos de la etnia indígena que ganan por encia de la media. Y ya teniendo eso en cuenta se procede a realizar un pivot de genero en ese data, en donde se agrupa y ordena por año con la intencion de saber cuantos hombres y mujeres por cada año ganaron por encima de la media.

// COMMAND ----------

// DBTITLE 1,Etnia Indígena
val dataMediaF2 = dataIndiMedia.groupBy("anio").pivot("genero").count().orderBy("anio")
dataMediaF2.show

// COMMAND ----------

// MAGIC %md
// MAGIC Para mejor visualización de los datos que se saco de la etnia indígena anteriormente con el pivot, se muestra en forma de gráfica los datos para saber y poder consultar de forma mas exacta en donde hay mas hombres o mujeres que ganaron por encima de la media.

// COMMAND ----------

display(dataMediaF2)
