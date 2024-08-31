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

def add_column():
    '''
    Função para adicionar coluna na tabela notas do banco de dados 'presenca'
    '''
    try:
        # Tenta conexão com banco de dados
        connection, cursor = connect_to_db()
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados : {error}")
        return
    
    try:
        # Executa a adição da nova coluna 'pesos'
        cursor.execute(
            """
            ALTER TABLE presenca.notas ADD COLUMN pesos SMALLINT
            """
        )
        connection.commit() # Confirma transação
        print("Coluna adicionada")

    except Exception as error:
        print(f"Erro ao adicionar tabela {error}")
        connection.rollback() # Reverte a transação em caso de erro
    finally:
        #Fecha o cursor
        cursor.close()
        connection.close()

def create_line(avaliacao, nota = -1.0, pesos = 0):
    '''
    Função para criar forma de avaliação atribuída na disciplina. Nota é opcional e padrão é -1.0
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            INSERT INTO presenca.notas(avaliacao, nota)
            VALUES (%s, %s, %s);
            """,
            (avaliacao, nota, pesos)
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
    '''
    Função para remover uma linha da tabela. Pode ser usada caso uma avaliação seja removida
    '''
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

def modify_line(avaliacao, nota, pesos = 0):
    '''
    Função criada para modificar uma linha da tabela. Pode ser usada para alterar a nota de uma avaliação
    '''
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

def correct_line(avaliacao, nota, pesos, avaliacao_correct, nota_correct, pesos_correct):
    '''
    Função para corrigir uma linha da tabela. Pode ser usada para corrigir uma avaliação
    '''
    try:
        connection, cursor = connect_to_db()
    except Exception as error:
        return
    try:
        cursor.execute(
            """
            UPDATE presenca.notas
            SET avaliacao = %s, nota = %s, pesos = %s
            WHERE avaliacao = %s
            """,
            (avaliacao_correct, nota_correct, pesos_correct, avaliacao)
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
    Função para visualizar os dados da tabela
    '''
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
