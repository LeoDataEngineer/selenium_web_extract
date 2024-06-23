import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import os 

def conectar_mysql():
    """Función para conectar a MySQL"""
    try:
        conn = mysql.connector.connect(
            user=os.environ['MYSQL_USER'],
            password=os.environ['MYSQL_PASSWORD'],
            host=os.environ['MYSQL_HOST'],
            database=os.environ['MYSQL_DATABASE'],
            port=14004  # Asegúrate de que este es el puerto correcto
        )
        print("Conexión a MySQL exitosa.")
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error de acceso: Usuario o contraseña incorrectos.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error de base de datos: La base de datos no existe.")
        else:
            print(err)
        return None

def crear_tabla(conn):
    """Crea la tabla 'producto' si no existe"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS producto (
                id_producto INT AUTO_INCREMENT PRIMARY KEY,
                id_registro INT,
                Empresa VARCHAR(100),
                producto INT,
                precio FLOAT,
                link VARCHAR(2000),
                xpath VARCHAR(2000),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("Tabla 'producto' creada exitosamente.")
    except mysql.connector.Error as err:
        print("Error al crear la tabla:", err)
    finally:
        cursor.close()

def cargar_datos_db(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'producto' en MySQL"""
    cursor = conn.cursor()
    try:
        data = [tuple(row) for row in df.values]
        insert_query = """
            INSERT INTO producto
            (id_registro, Empresa, producto, precio, link, xpath)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        conn.commit()
        print("Datos cargados en MySQL.")
    except mysql.connector.Error as err:
        print("Error al cargar datos en MySQL:", err)
        conn.rollback()
    finally:
        cursor.close()

def main():
    conn = conectar_mysql()
    if conn:
        crear_tabla(conn)
        df = pd.read_csv('productos.csv')
        df.columns = df.columns.str.lower()  # Asegúrate de que los nombres de las columnas coinciden con los nombres en la tabla
        cargar_datos_db(conn, df)
        conn.close()

if __name__ == "__main__":
    main()
