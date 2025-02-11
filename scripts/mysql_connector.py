import pandas as pd
import mysql.connector
import numpy as np

# Configuración de conexión a MySQL
MYSQL_HOST = "localhost"
MYSQL_PORT = 3307
MYSQL_USER = "root"
MYSQL_PASSWORD = "112358"
DATABASE_NAME = "instacart_db"
BATCH_SIZE = 1000  # Ajusta según necesidad


# Conectar a MySQL
def conectar_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        print("Conexión a MySQL exitosa.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error al conectar a MySQL: {err}")
        return None


# Crear base de datos
def crear_base_datos(conn):
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    conn.database = DATABASE_NAME
    print(f"Base de datos '{DATABASE_NAME}' lista.")
    cursor.close()


# Función para mapear tipos de Pandas a MySQL
def inferir_tipo_mysql(df):
    tipo_mysql = {}
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            tipo_mysql[col] = "INT"
        elif pd.api.types.is_float_dtype(df[col]):
            tipo_mysql[col] = "FLOAT"
        elif pd.api.types.is_bool_dtype(df[col]):
            tipo_mysql[col] = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            tipo_mysql[col] = "DATETIME"
        elif df[col].nunique() < 10:
            valores = "', '".join(df[col].dropna().astype(str).unique())
            tipo_mysql[col] = f"ENUM('{valores}')"
        else:
            max_length = df[col].astype(str).apply(len).max()
            tipo_mysql[col] = f"VARCHAR({min(max_length, 255)})"

    return tipo_mysql


# Crear tablas y cargar datos en lotes
def crear_tablas_y_cargar_datos(conn, data_dict):
    cursor = conn.cursor()

    for name, df in data_dict.items():
        tipos = inferir_tipo_mysql(df)
        columnas = ", ".join([f"`{col}` {tipo}" for col, tipo in tipos.items()])
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{name}` ({columnas})"
        cursor.execute(create_table_query)
        print(f"Tabla '{name}' creada.")

        # Convertir NaN a None para compatibilidad con MySQL
        df = df.replace({np.nan: None})

        # Crear sentencia SQL de inserción
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO `{name}` VALUES ({placeholders})"

        # Insertar en lotes
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
        for i in range(0, len(data_tuples), BATCH_SIZE):
            batch = data_tuples[i:i + BATCH_SIZE]
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"{len(batch)} filas insertadas en '{name}'.")

    cursor.close()


if __name__ == "__main__":
    # Importar data
    db1 = pd.read_csv('/Users/lu/Documents/202402/Data_Mining/proyecto_1/data/aisles.csv', sep=';')
    db2 = pd.read_csv("/Users/lu/Documents/202402/Data_Mining/proyecto_1/data/departments.csv", sep=';')
    db3 = pd.read_csv("/Users/lu/Documents/202402/Data_Mining/proyecto_1/data/instacart_orders.csv", sep=';')
    db4 = pd.read_csv("/Users/lu/Documents/202402/Data_Mining/proyecto_1/data/order_products.csv", sep=';')
    db5 = pd.read_csv("/Users/lu/Documents/202402/Data_Mining/proyecto_1/data/products.csv", sep=';')

    # Diccionario de datos
    data = {'aisles': db1, 'departments': db2, 'instacart_orders': db3, 'order_products': db4, 'products': db5}

    # Conexión a MySQL
    conexion = conectar_mysql()

    try:
        if conexion:
            crear_base_datos(conexion)
            conexion.database = DATABASE_NAME
            crear_tablas_y_cargar_datos(conexion, data)
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if conexion and conexion.is_connected():
            conexion.close()
            print("Conexión cerrada.")
