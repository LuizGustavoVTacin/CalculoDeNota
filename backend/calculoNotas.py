import db_notas as notas
import db_presenca as presenca

def captura_avaliacao():

    try:
        # Tenta conexão com banco de dados
        connection, cursor = notas.connect_to_db()
    except Exception as error:
        print(f"Erro ao conectar ao banco de dados : {error}")
        return



