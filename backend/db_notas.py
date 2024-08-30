import psycopg2
from credenciais import credenciais_db

def connect_to_db():

    db = credenciais_db()

    try:
        connection  = psycopg2.connect(
            host=db.host,
            database=db.database,
            user=db.user,
            password=db.password,
            port=db.port
        )
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("Conectado a:", record)
        return connection, cursor

    except Exception as error:
        print("Erro ao conectar ao banco de dados", error)
        return

def create_schema():

    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS presenca;
            """
        )
        connection.commit()
        print("Schema criado com sucesso")
    except Exception as error:
        print("Erro ao criar schema", error)
        return
    finally:
        cursor.close()
        connection.close()

def create_table():

    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS presenca.notas(
                avaliacao VARCHAR(11) NOT NULL,
                nota NUMERIC(4,2) NOT NULL
            );
            """
        )
        connection.commit()
        print("Tabela criada com sucesso")

    except Exception as error:
        print("Erro ao criar tabela", error)
        return
    finally:
        cursor.close()
        connection.close()

def create_line(avaliacao, nota = -1.0):
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            INSERT INTO presenca.notas(avaliacao, nota)
            VALUES (%s, %s);
            """,
            (avaliacao, nota)
        )
        connection.commit()
        print("Linha criada com sucesso")

    except Exception as error:
        print("Erro ao criar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def drop_line(avaliacao):
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            DELETE FROM presenca.notas
            WHERE avaliacao = %s
            """,
            (avaliacao)
        )
        connection.commit()
        print("Linha deletada com sucesso")

    except Exception as error:
        print("Erro ao deletar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def modify_line(avaliacao, nota):
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            UPDATE presenca.notas
            SET nota = %s
            WHERE avaliacao = %s;
            """,
            (nota, avaliacao)
        )
        connection.commit()
        print("Linha modificada com sucesso")

    except Exception as error:
        print("Erro ao modificar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def correct_line(avaliacao, nota, avaliacao_correct, nota_correct):
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            UPDATE presenca.notas
            SET avaliacao_correct = %s, nota_correct = %s
            WHERE avaliacao = %s AND nota = %s;
            """,
            (avaliacao_correct, nota_correct, avaliacao, nota)
        )
        connection.commit()
        print("Linha corrigida com sucesso")

    except Exception as error:
        print("Erro ao corrigir linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def view_data():
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            SELECT * FROM presenca.notas;
            """
        )
        records = cursor.fetchall()
        for record in records:
            print(record)
        return records

    except Exception as error:
        print("Erro ao visualizar dados", error)
        return
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    view_data()