// Databricks notebook source
// DBTITLE 1,Especificación del tipo de dato
// MAGIC %md
// MAGIC Para este apartado de la creación del dataFrame, primeramente se debe importar org.apache.spark.sql.types._ con la finalidad de que nos permita darle a cada columna el tipo de dato de nuestra preferencia, pero teniendo en cuenta que datos se guardan en ella, por ejemplo si esa columna tiene datos cadena no se le va a decir que sea de tipo IntegerType sino que aqui obligadamente debe ser StringType.
// MAGIC 
// MAGIC En la línea dos observamos la importación del .sql.types._ ya mencionado y a continuación dentro de una variable myDataSchema vamos a guardar toda la estructura y cambios que queremos que tengan nuestro data para su creación.
// MAGIC 
// MAGIC Dentro de un arreglo (línea de codigo 4 a la 22) especificamos las estructuras para el data, en donde en primer lugar por cada columna que tiene el archivo .csv que se va a convertir en DataFrame se debe colocar StructField() y dentro de este primeramente el nombre de la columna, seguido del tipo de dato, en esta parte se debe colocar el tamaño máximo que queremos que tenga esa columna solo cuando especifiques un dato decimal por ejemplo la línea de código 5 en otros casos no viene a ser necesario y por último si deseas que esta tenga datos nulos (tru para que si tenga datos nulos y false para que no tenga datos nulos).

// COMMAND ----------

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

// DBTITLE 1,Creación y lectura del DataFrame
// MAGIC %md
// MAGIC Una vez especificado los tipos de datos para las columnas, se procede a crear el data y guardarla dentro de una variable. En donde para la creación se usan funciones como: read que se deriva de spark que es un objeto para leer el csv, schema para reemplazar la estructura con la creada anteriormente, en option le damos configuraciones a tener en cuenta al momento de crearse, el header sirve para indicar si queremos o no que aparesca el nombre de la columna y con delimiter le especificamos el separador para cada columna y por ultimo el .csv tendra la ruta del archivo ya cargado en el databricks con antelación.

// COMMAND ----------

val data = spark
  .read
  .schema(myDataSchema)
  .option("header", "true")
  .option("delimiter", "\t")
  .csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv");

// COMMAND ----------

// DBTITLE 1,Consulta de la cantidad de datos existentes en el DataFrame
// MAGIC %md
// MAGIC A través de un println se imprimira con la ayuda del .count que sirve para contar la cantidad de filas existentes en el DataFrame.

// COMMAND ----------

//Saber la cantidad de filas que hay
println($"TOTAL DE DATOS = ${data.count}")

// COMMAND ----------

// DBTITLE 1,Consulta para determinar si existen datos nulos
// MAGIC %md
// MAGIC Para esta cosulta se utiliza sql, por lo cual primeramente se debe crear una vista temporal de la siguiente forma: 'nombre del Data'.createOrReplaceTempView('nombre de la vista'). Luego para realizar la consulta se hace el llamado a spark.sql() y dentro de los parentesis entre el medio de tres comillas tanto al inicio como al final se escribe la consulta sql, pero ya fuera del parentesis se puede seguir escribiendo codigo de scala como es en nuestro caso el show() que sirve para mostrar los resultados.

// COMMAND ----------

data.createOrReplaceTempView("AnalisisSQL1")

// COMMAND ----------

spark.sql("""
  SELECT ingreso_laboral, COUNT(*) total
  FROM AnalisisSQL1
  GROUP BY ingreso_laboral
  ORDER BY COUNT(*) DESC
""").show(4)

// COMMAND ----------

// DBTITLE 1,Eliminación de datos nulos
// MAGIC %md
// MAGIC Como podemos observar en la consulta anterior realizada con spark.sql() si existen datos nulos en el data, por lo que se procede a realizar una nueva data que no contengan a estos, en donde primeramente se ha seleccionado las variables o columnas con las que se va a trabajar y seguido de esto un where para decirle que no registre los datos que tengan en la variable ingreso_laboral el valor de null y para esto nos ayudamos de la funcion .isNotNull.

// COMMAND ----------

val newData = data.select("id_persona", "anio", "area", "genero", "etnia", "ingreso_laboral", "grupo_ocupacion", "rama_actividad").where($"ingreso_laboral".isNotNull)
println(newData.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Con la finalidad de saber que valores aparecen con mayor probabilidad y cual es el máximo y mínimo se realiza un summary luego de realizar una consulta sql que selecciona todos los datos de la variable ingreso_laboral y finalmente con el .show() se muestra los resultados.

// COMMAND ----------

newData.createOrReplaceTempView("AnalisisSQL2")
spark.sql("""
  SELECT ingreso_laboral
  FROM AnalisisSQL2
""").summary().show

// COMMAND ----------

// MAGIC %md
// MAGIC Dividimos los datos en rangos para saber cuales son los que mas hay.
// MAGIC 
// MAGIC La variable minValor es igual a cero y maxvalor es igual a 146030 puesto que en el summary anterior se pudo observar cual es el mínimo y cual es el máximo valor que hay en nuestro data con referencia al ingreso_laboral. Y para terminar dentro del while vamos contando cuantos datos hay dentro de los rangos (la variable bins contiene la cantidad de rangos que vamos a tener) y luego los presentamos.

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

// DBTITLE 1,Importación de paquete para utilizar funciones de sql en spark
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Calculo de la media y la desviación estándar 
// MAGIC %md
// MAGIC Con la finalidad de saber el promedio de los valores de la variable ingreso_laboral se usa el función mean, especificandole entre parentesis la variable de la cual queremos que saque los resultados. Por otra parte para sacar la desviación estándar se usa la función stddev en donde al igual que la anterior entre parentesis le damos la variable y por ultimo transformamos el resultado a double con la función .asInstanceOf[Double]

// COMMAND ----------

val avg = newData.select(mean("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

val stdDev = newData.select(stddev("ingreso_laboral")).first()(0).asInstanceOf[Double]

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos los limites.
// MAGIC 
// MAGIC Con la ayuda de la media y la desviación estándar calculamos los limites de nuestros valores de ingreso_laboral, en donde primeramente nuestro limInferior se puede observar que tiene un valor negativo y limSuperior un valor positivo, lo cual quiere decir que nuestros datos sin outliers deben estar entre estos rangos, y los demas datos ya sean menores al limInferior o mayores al limSuperior son los datos que se encuentran por asi decirlo de manera excesiva.

// COMMAND ----------

val limInferior = avg - 3 * stdDev
val limSuperior = avg + 3 * stdDev

// COMMAND ----------

// MAGIC %md
// MAGIC Este filtro se hace solo con la intención de saber si existen datos por debajo del limInferior. Pero como se ve en los resultados no hay ninguno puesto que no hay un ingreso laboral que sea negativo.

// COMMAND ----------

// filtro de datos menos al limInferior
val valInferiorAllimInferior = newData.where($"ingreso_laboral" < limInferior)

// COMMAND ----------

valInferiorAllimInferior.createOrReplaceTempView("AnalisisSQL3")
spark.sql("""
  SELECT *
  FROM AnalisisSQL3
""").describe().show

// COMMAND ----------

// MAGIC %md
// MAGIC En este filtro se buscan los datos que sean mayores al limSuperior. Aqui hay que tener algo en cuenta y es que estos datos al ser ya mayores al limSuperior se consideran excesivos por lo que ya se consideran datos outliers.

// COMMAND ----------

// filtro de datos mayores al limSuperior
val valMayorAllimSuperior = newData.where($"ingreso_laboral" > limSuperior)

// COMMAND ----------

valMayorAllimSuperior.createOrReplaceTempView("AnalisisSQL4")
spark.sql("""
  SELECT *
  FROM AnalisisSQL4
""").describe().show

// COMMAND ----------

// DBTITLE 1,Data sin Outliers
// MAGIC %md
// MAGIC A continuación guardamos solamente los datos que se encuentran entre estos limites ya mencionados anteriormente, ya que los que no se encuentran entre estos se consideran datos outliers y se quitarian de nuestra data, y para esto dentro de un where le decimos que seleccione solo aquellos datos que tengan un ingreso_laboral mayor al limInferior y memor al limSuperior.

// COMMAND ----------

val newDataSinOutliers = newData.where($"ingreso_laboral" > limInferior && $"ingreso_laboral" < limSuperior)

// COMMAND ----------

// MAGIC %md
// MAGIC Con la ayuda de una consulta sql, en la cual la vista temporal apunta a la data sin outliers (newDataSinOutliers) se procede a seleccionar la variable ingreso_laboral con la finalidad de realizarle un summary() y mostrar ahora que dato es el mínimo, máxiomo y con que probabilidad aparecen segun el rango.

// COMMAND ----------

newDataSinOutliers.createOrReplaceTempView("AnalisisSQL5")

// COMMAND ----------

spark.sql("""
  SELECT ingreso_laboral
  FROM AnalisisSQL5
""").summary().show

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora que ya hemos confirmado que el data nuevo ya no tiene datos outliers se procede a consultar las etnias con ayuda de una consulta sql en donde le decimos que por cada etnia cuente cuando datos existen y que los muestre de mayor a menor.

// COMMAND ----------

spark.sql("""
  SELECT etnia, COUNT(*) total
  FROM AnalisisSQL5
  GROUP BY etnia
  ORDER BY COUNT(*) DESC
""").show

// COMMAND ----------

// MAGIC %md
// MAGIC 1.Cuantos son los hombres y mujeres que hay en cada una de las 2 primeras etnias mas nombradas en cada año

// COMMAND ----------

// MAGIC %md
// MAGIC Como ya sabemos de que etnias hay mas datos gracias a la consulta anterior realizada con sql, se procede a guardar en un data diferente para cada etnia toda la información que le corresponde de nuestro newDataSinOutliers.

// COMMAND ----------

val dataEtniaMas1 = newDataSinOutliers.where($"etnia" === "6 - Mestizo")
val dataEtniaMas2 = newDataSinOutliers.where($"etnia" === "1 - Indígena")

// COMMAND ----------

// MAGIC %md
// MAGIC En las variables ya mencionadas se hace un pivote de la variable género donde tomara los únicos valores que poseea esta variable, además se lo agrupara por año para saber por los distintos años en donde se encuentra la mayor cantidad de hombres y mujeres que han respondido la encuesta que se haya realizado para obtener este data y se lo ordenara por esta mismo para una mejor visualización en tablas para cada una de las dos datas 

// COMMAND ----------

val dataGeneroMestizos = dataEtniaMas1.groupBy("anio").pivot("genero").count().orderBy("anio")
val dataGeneroIndigenas = dataEtniaMas2.groupBy("anio").pivot("genero").count().orderBy("anio")

println("Mestizos")
dataGeneroMestizos.show
println("Indígenas")
dataGeneroIndigenas.show

// COMMAND ----------

// MAGIC %md
// MAGIC En la anterior consulta se lo pudo ver en lo que son tablas, ahora se lo presentará por gráficas con la función display, con la intencion de saber de manera mas exacta en que año hubo mas hombres y en cual mas mujeres, puesto que la anterior se ordenaba por el año y no se sabia en donde se habia encuestado mas hombres que mujeres o viceversa.

// COMMAND ----------

// DBTITLE 1,Gráfica de Mestizos
display(dataGeneroMestizos)

// COMMAND ----------

// DBTITLE 1,Gráfica de Indígenas
display(dataGeneroIndigenas)

// COMMAND ----------

// MAGIC %md
// MAGIC 2.Cuantos hombres y mujeres hay en el área urbana y rural de cada una de las 2 etnias.

// COMMAND ----------

// MAGIC %md
// MAGIC Para responder a la siguiente pregunta con la ayuda de consultas sql se procede a sacar los datos unicos de la variable area.

// COMMAND ----------

spark.sql("""
    SELECT DISTINCT area
    FROM AnalisisSQL1
""").show

// COMMAND ----------

// MAGIC %md
// MAGIC Tomando en cuenta las 2 primeras datas en donde se tiene ya las 2 etnias mas mencionadas se procede a realizar un pivot de la variable género con la intencion de saber en donde hay mas hombres y mujeres.

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
// MAGIC 3.Cual es el valor promedio de ingreso que ganan los hombres y mujeres del área urbana y rural de cada una de las 2 etnias mas nombradas

// COMMAND ----------

// MAGIC %md
// MAGIC Trabajando con las 2 datas de las etnias se procede a hacer un pivote de la variable género y para poder responder la pregunta ya mencionada se saca el valor promedio de la variable ingreso_laboral y como se desea saber el area se la agrupa y ordena por esta mismo

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
// MAGIC 4.Cual es la ocupación mas nombrada entre los hombres y mujeres de las 2 etnias en el área urbana y rural

// COMMAND ----------

// DBTITLE 1,Ocupaciones de los Mestizos
// MAGIC %md
// MAGIC Se realiza una consulta sql para cada una de las variables que intervienen en el enunciado en donde la vista temporal se la realiza a la dataEtniaMas1 que contiene todos los datos de los mestizos, con la finalidad de saber tanto del area urbana como rural en donde se encuentran trabajando mas los hombres y las mujeres.

// COMMAND ----------

dataEtniaMas1.createOrReplaceTempView("ocupacioMestizosSQL")
val dataOcupacionMestizosU_H = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacioMestizosSQL
  WHERE genero = '1 - Hombre'
    AND area = '1 - Urbana'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val dataOcupacionMestizosU_M = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacioMestizosSQL
  WHERE genero = '2 - Mujer'
    AND area = '1 - Urbana'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val  dataOcupacionMestizosR_H = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacioMestizosSQL
  WHERE genero = '1 - Hombre'
    AND area = '2 - Rural'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val  dataOcupacionMestizosR_M = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacioMestizosSQL
  WHERE genero = '2 - Mujer'
    AND area = '2 - Rural'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

println("Ocupacion Hombres area Urbana")
dataOcupacionMestizosU_H.show(false)
println("Ocupacion Mujeres area Urbana")
dataOcupacionMestizosU_M.show(false)
println("Ocupacion Hombres area Rural")
dataOcupacionMestizosR_H.show(false)
println("Ocupacion Mujeres area Rural")
dataOcupacionMestizosR_M.show(false)

// COMMAND ----------

// DBTITLE 1,Ocupaciones de los Indígenas
// MAGIC %md
// MAGIC Se realiza la misma consulta en sql que la anterior, pero aquí la vista temporal es de la data dataEtniaMas2 que contiene solo los datos de los indígenas.

// COMMAND ----------

dataEtniaMas2.createOrReplaceTempView("ocupacionIndigenasSQL")
val dataOcupacionIndigenasU_H = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacionIndigenasSQL
  WHERE genero = '1 - Hombre'
    AND area = '1 - Urbana'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val dataOcupacionIndigenasU_M = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacionIndigenasSQL
  WHERE genero = '2 - Mujer'
    AND area = '1 - Urbana'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val  dataOcupacionIndigenasR_H = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacionIndigenasSQL
  WHERE genero = '1 - Hombre'
    AND area = '2 - Rural'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

val  dataOcupacionIndigenasR_M = spark.sql("""
  SELECT grupo_ocupacion , COUNT(*) as total
  FROM ocupacionIndigenasSQL
  WHERE genero = '2 - Mujer'
    AND area = '2 - Rural'
  GROUP BY grupo_ocupacion
  ORDER BY COUNT(*) DESC
""")

println("Ocupacion Hombres area Urbana")
dataOcupacionIndigenasU_H.show(false)
println("Ocupacion Mujeres area Urbana")
dataOcupacionIndigenasU_M.show(false)
println("Ocupacion Hombres area Rural")
dataOcupacionIndigenasR_H.show(false)
println("Ocupacion Mujeres area Rural")
dataOcupacionIndigenasR_M.show(false)
