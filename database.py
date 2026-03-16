import oracledb
import logging

# Credenciales (puedes moverlas a un archivo .env después)
USER = "system"
PASSWORD = "system"
DSN = "localhost:1521/XE"

def insertar_productos(df):
    connection = None
    try:
        logging.info("Conectando a Oracle 21c...")
        connection = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
        cursor = connection.cursor()

        # Aseguramos el orden exacto de las columnas
        df_para_oracle = df[['Nombre', 'Precio', 'Marca']]
        registros = list(df_para_oracle.itertuples(index=False, name=None))

        sql = "INSERT INTO Smartphone_Lider (NOMBRE, PRECIO, MARCA) VALUES (:1, :2, :3)"
        
        cursor.executemany(sql, registros)
        connection.commit()
        logging.info(f"✅ Éxito: Se insertaron {len(registros)} registros en Oracle.")

    except Exception as e:
        logging.error(f"❌ Error al insertar en Oracle: {e}")
        raise e # Re-lanzamos el error para que main lo detecte
    finally:
        if connection:
            cursor.close()
            connection.close()