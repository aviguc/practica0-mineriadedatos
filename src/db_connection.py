def get_db_properties():
    url="jdbc:mysql://localhost:3306/IBEX35?useSSL=false&serverTimezone=UTC"
    propiedades={"driver": "com.mysql.cj.jdbc.Driver", "user":"root", "password": "Alicante07"}
    return url, propiedades

def guardar_tabla_jdbc(df, nombre_tabla, modo="overwrite"):
    url, propiedades=get_db_properties()
    df.write.jdbc(url=url, table=nombre_tabla, mode=modo, properties=propiedades)