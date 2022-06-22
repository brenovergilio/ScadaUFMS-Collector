import psycopg2 as db
from threading import Lock


class Config():
    """
    Classe de configurações de conexão POSTGRES
    """

    def __init__(self):
        self.config = {
            "postgres": {
                "user": "postgres",
                "password": "ufms123",
                "host": "127.0.0.1",
                "port": "5432",
                "database": "testmm"
            }
        }


class Connection(Config):
    """
    Classe para iniciar a conexão com o banco de dados
    """

    def __init__(self):
        Config.__init__(self)
        try:
            self._con = db.connect(**self.config["postgres"])
            self._cursor = self._con.cursor()
        except Exception as e:
            print('Erro de conexão com o banco: ', e.args)
            exit(1)


class DBHandler(Connection):
    """
    Classe para a manipulação do banco de dados
    """

    def __init__(self):
        Connection.__init__(self)
        self._lock = Lock()
        self._cursor.execute("SET TIMEZONE='Brazil/West';")
        self.create_medidores()
        self.create_medicoes()
        self.create_feriado()
        self.create_missing_medicoes_md30()
        self.create_alarmes()

    def __del__(self):
        self._con.commit()
        self._con.close()

    # Métodos de criação de tabela

    def create_medidores(self):
        """
        Método responsável por criar a tabela de medidores, caso esta não exista
        """
        try:
            sql_str = f"""
      CREATE TABLE IF NOT EXISTS medidores_md30 (
        id UUID PRIMARY KEY,
        ip TEXT NOT NULL,
        created_at TIMESTAMP,
        nome TEXT NOT NULL,
        porta INTEGER NOT NULL,
        hora_ponta INTEGER NOT NULL,
        minuto_ponta INTEGER NOT NULL,
        intervalo_ponta INTEGER NOT NULL
      );
      """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def create_medicoes(self):
        """
        Método responsável por criar a tabela que armazena os dados coletados pelos medidores
        """
        try:
            sql_str = f"""
      CREATE TABLE IF NOT EXISTS medicoes_md30 (
        medidor_id UUID NOT NULL,
        timestamp TIMESTAMP PRIMARY KEY,
        tensao_fase_a REAL NOT NULL,
        tensao_fase_b REAL NOT NULL,
        tensao_fase_c REAL NOT NULL,
        corrente_fase_a REAL NOT NULL,
        corrente_fase_b REAL NOT NULL,
        corrente_fase_c REAL NOT NULL,
        potencia_ativa_total REAL NOT NULL,
        potencia_reativa_total REAL NOT NULL,
        FOREIGN KEY (medidor_id) REFERENCES medidores_md30(id) ON DELETE CASCADE ON UPDATE CASCADE
      );
      """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def create_feriado(self):
        """
        Método responsável por criar a tabela que registra os feriados
        """
        try:
            sql_str = f"""
        CREATE TABLE IF NOT EXISTS feriados (
            id UUID PRIMARY KEY,
            nome TEXT NOT NULL,
            dia DATE NOT NULL
        );
        """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def create_missing_medicoes_md30(self):
        """
        Método responsável por criar a tabela que registra os timestamps das medições que não foram concluídas
        """
        try:
            sql_str = f"""
        CREATE TABLE IF NOT EXISTS missing_medicoes_md30 (
            medidor_id UUID NOT NULL,
            unix_timestamp INTEGER NOT NULL,
            FOREIGN KEY (medidor_id) REFERENCES medidores_md30(id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def create_alarmes(self):
        """
        Método responsável por criar a tabela que registra os alarmes dos medidores
        """
        try:
            sql_str = f"""
        CREATE TABLE IF NOT EXISTS alarmes (
            id UUID PRIMARY KEY,
            medidor_id UUID NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            message TEXT NOT NULL,
            FOREIGN KEY (medidor_id) REFERENCES medidores_md30(id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    # Métodos de inserção

    def add_medicoes(self, id, timestamp, medicoes):
        """
        Método responsável por adicionar as medições do medidor no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{id}', '{timestamp}', " + ','.join((str(v)
                                                                for v in medicoes))
            sql_str = f"INSERT INTO medicoes_md30 VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_alarme(self, id, medidor_id, timestamp, message):
        """
        Método responsável por adicionar um novo alarme no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{id}', '{medidor_id}','{timestamp}', '{message}'"
            sql_str = f"INSERT INTO alarmes (id, medidor_id, timestamp, message) VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_missing_medicao_md30(self, medidor_id, unix_timestamp):
        """
        Método responsável por adicionar um registro de medição que falhou
        """
        try:
            self._lock.acquire()
            str_values = f"'{medidor_id}', '{unix_timestamp}'"
            sql_str = f"INSERT INTO alarmes (medidor_id, unix_timestamp) VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    # Métodos de deleção
    def delete_missing_medicao_md30(self, medidor_id, unix_timestamp):
        """
        Método responsável por deletar um registro de medição que falhou
        """
        try:
            self._lock.acquire()
            sql_str = f"DELETE FROM missing_medicoes_md30 WHERE medidor_id='{medidor_id}' AND unix_timestamp='{unix_timestamp}'"
            self._cursor.execute(sql_str)
            self._con.commit()
            return True
        except Exception as e:
            print("Erro: ", e.args)
            return False
        finally:
            self._lock.release()

    # Métodos de seleção

    def get_all_medidores(self):
        """
        Retorna todos os medidores cadastrados
        """
        try:
            self._lock.acquire()
            sql_str = "SELECT * FROM medidores_md30;"
            self._cursor.execute(sql_str)
            medidores = self._cursor.fetchall()
            self._con.commit()
            return medidores
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def get_all_missing_medicoes_md30(self, medidor_id):
        """
        Retorna todos os medidores cadastrados
        """
        try:
            self._lock.acquire()
            sql_str = f"SELECT * FROM missing_medicoes_md30 WHERE medidor_id='{medidor_id}';"
            self._cursor.execute(sql_str)
            missing = self._cursor.fetchall()
            self._con.commit()
            return missing
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()
