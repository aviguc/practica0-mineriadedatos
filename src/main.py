from spark_session import get_spark_session
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.window import * 
from db_connection import guardar_tabla_jdbc

spark=get_spark_session()

#Ej1-a
print("Ej1-a")
df=(
    spark.read
    .option("header", True)
    .option("sep", ";")
    .csv("../data/ibex35_close-2024.csv")
)

# Mostrar el esquema inicial
df.printSchema()
df_transformado=df.withColumn("Fecha", to_date(col("Fecha"), "dd/MM/yyyy"))
for columna in df_transformado.columns:
    if columna != "Fecha":
        df_transformado=df_transformado.withColumn(columna, col(f"`{columna}`").cast(DoubleType())
        )
df_transformado.printSchema()
df_transformado.show(6)

#Ej1-b
print("Ej1-b")
for columna in df_transformado.columns:
    if ".MC" in columna:
        nuevo=columna.replace(".MC", "")
        df_transformado=df_transformado.withColumnRenamed(columna, nuevo)
df_transformado.show(6)


#Ej2-a
print("Ej2-a")
principio=df_transformado.count()
df_duplicados=df_transformado.distinct()
final=df_duplicados.count()
cantidad=principio-final
print(cantidad)
empresas_disponibles=len(df_duplicados.columns)-1
#como en el csv las columnas son la de la fecha y luego cada empresa, el numero de empresas con informacion disponible sera columnas - 1 (la columna de la fecha)
print(empresas_disponibles)


#Ej2-b
print("Ej2-b")
df_duplicados.select(min("Fecha").alias("Fecha inicial"), max("Fecha").alias("Fecha final")).show()
dias_disponibles=df_duplicados.count()
print(dias_disponibles)
print("Si me parece coherente, ya que si quitas fines de semana y dias festivos, 255 es el número que sale mas o menos.")
print("No veo necesario coger datos adicionales porque en los fines de semana y festivos no hay precio de cotización de las empresas.")


#Ej3
print("Ej3")
df_renombrado=df_transformado.withColumnRenamed("Fecha", "Dia")
df_renombrado.show(10)
empresas= []
for c in df_renombrado.columns:
    if c!= "Dia":
        empresas.append(c)
for empresa in empresas:
    df_renombrado.select(avg(col(empresa)).alias("Media anual"), max(col(empresa)).alias("Max anual"), min(col(empresa)).alias("Min anual")).show()
df_3=df_renombrado.withColumn("Deficiency Notice UNI", when(col("UNI")<1.0, True).otherwise(False))
df_3.show(100)

#Ej4
print("Ej4")
df_ordenado=df_renombrado.orderBy("Dia")
precio_inicial=df_ordenado.head(1)[0]
precio_final=df_ordenado.tail(1)[0]
datos=[]
for empresa in empresas:
    valor_inicial=precio_inicial[empresa]
    valor_final=precio_final[empresa]
    if valor_inicial is not None and valor_final is not None:
        variacion=(((valor_final-valor_inicial)/valor_inicial)*100)
        datos.append((empresa,variacion))

df_variacion=spark.createDataFrame(datos,["Empresa", "Variacion Anual"])
df_resultado=df_variacion.withColumn(
    "Clasificación",
    when(col("Variacion Anual")<=-15.0,"Bajada Fuerte")
    .when((col("Variacion Anual")>-15.0) & (col("Variacion Anual")<-1.0),"Bajada")
    .when((col("Variacion Anual")>=-1.0) & (col("Variacion Anual")<=1.0),"Neutra")
    .when((col("Variacion Anual")>1.0) & (col("Variacion Anual")<15.0),"Subida")
    .otherwise("Subida Fuerte")
)
df_resultado.show(40,truncate=False)

#Ej5
print("Ej5")
for empresa in empresas:
    cuartiles = df_renombrado.approxQuantile(empresa, [0.25, 0.5, 0.75], 0.01)
    q1=cuartiles[0]
    q2=cuartiles[1]
    q3=cuartiles[2]
    nombre_columna=f"{empresa} Cuartil"
    df_renombrado=df_renombrado.withColumn(
        nombre_columna,
        when(col(empresa)<=q1,"q1")
        .when(col(empresa)<=q2,"q2")
        .when(col(empresa)<=q3,"q3")
        .otherwise("q4")
    )
df_renombrado.show(1)
columnas_solicitadas=["Dia", "AENA", "AENA Cuartil", "BBVA", "BBVA Cuartil"]
total=df_renombrado.count()

df_renombrado.select(columnas_solicitadas).show(total,truncate=False)



print("\n Almacenamiento en base de datos SQL")
try:
    print("Exportando datos originales (CSV) a la tabla 'Datos2024'...")
    guardar_tabla_jdbc(df, "Datos2024, modo=overwrite")
    print("Tabla 'Datos2024' almacenada correctamente")
except Exception as e:
    print(f"No se pudo conectar a MySQL para guardar datos brutos ({e})")

try:
    print("Exportando datos tratados (sin nuevas columnas) a la tabla 'Datos2024'...")
    guardar_tabla_jdbc(df_duplicados, "Datos2024", modo="overwrite")
    print("Tabla 'Datos2024' (datos tratados) actualizada correctamente")
except Exception as e:
    print(f"No se pudo conectar a MySQL para guardar datos tratados ({e})")