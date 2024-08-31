import psycopg2
from credenciais import credenciais_db

def connect_to_db():
    '''
    Função para conectar ao banco de dados
    '''
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
    '''
    Função para criar schema no banco de dados
    '''
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
    '''
    Função para criar tabela no banco de dados
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS presenca.presenca(
                dia DATE NOT NULL,
                nome VARCHAR(60) NOT NULL,
                Fui SMALLINT DEFAULT 0,
                Passou_Lista SMALLINT DEFAULT 0,
                Assinaram SMALLINT DEFAULT 0
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

def create_line(dia, nome="ICC 2", fui = 0, passou_lista = 0, assinaram = 0):
    '''
    Função para adicionar uma linha com aula e dia. Colocar se fui, passou lista e assinaram é opcional
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            INSERT INTO presenca.presenca(dia, nome, fui, passou_lista, assinaram)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (dia, nome, fui, passou_lista, assinaram)
        )
        connection.commit()
        print("Linha criada com sucesso")

    except Exception as error:
        print("Erro ao criar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def drop_line(dia, nome):
    '''
    Função para deletar uma linha com aula e dia
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            DELETE FROM presenca.presenca
            WHERE dia = %s AND nome = %s;
            """,
            (dia, nome)
        )
        connection.commit()
        print("Linha deletada com sucesso")

    except Exception as error:
        print("Erro ao deletar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def modify_line(dia, nome, fui, passou_lista, assinaram):
    '''
    Função para modificar o status de fui, passou lista e assinaram de uma linha
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            UPDATE presenca.presenca
            SET fui = %s, passou_lista = %s, assinaram = %s
            WHERE dia = %s AND nome = %s;
            """,
            (fui, passou_lista, assinaram, dia, nome)
        )
        connection.commit()
        print("Linha modificada com sucesso")

    except Exception as error:
        print("Erro ao modificar linha", error)
        return
    finally:
        cursor.close()
        connection.close()

def correct_line(dia, nome, dia_correct, nome_correct):
    '''
    Função para corrigir o dia e nome de uma linha
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            UPDATE presenca.presenca
            SET dia = %s, nome = %s
            WHERE dia = %s AND nome = %s;
            """,
            (dia_correct, nome_correct, dia, nome)
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
    '''
    Função para visualizar todos os dados da tabela
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            SELECT * FROM presenca.presenca ORDER BY DIA ASC;
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