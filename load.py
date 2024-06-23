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
            port=14004
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
    """Crea la tabla 'subtedata' si no existe"""
    cursor = conn.cursor()
    try:
        # Primero, intenta eliminar la tabla si existe
        # cursor.execute("CREATE TABLE IF NOT EXISTS producto")
        # Luego, crea la nueva tabla
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
        # Confirma los cambios
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
        # Convertir el DataFrame de pandas en una lista de tuplas para la inserción
        data = [tuple(row) for row in df.values]
        # Preparar la consulta de inserción
        insert_query = """
                        INSERT INTO subtedata
                        (id_registro, Empresa, producto, link, xpath)
                        VALUES (%s, %s, %s, %s, %s)
                        """
        # Insertar los datos en lotes para mejorar el rendimiento
        cursor.executemany(insert_query, data)
        # Confirmar los cambios
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
        
        # Asumiendo que 'productos.csv' es el archivo con los datos extraídos
        df = pd.read_csv('productos.csv')
        df.columns = df.columns.str.upper()

        cargar_datos_db(conn, df)
        
        # Cerrar la conexión
        conn.close()

if __name__ == "__main__":
    main()
