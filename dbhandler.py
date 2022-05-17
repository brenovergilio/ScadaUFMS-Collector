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
                "database": "test"
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
        self.create_users()
        self.create_medidores()
        self.create_medicoes()
        self.create_feriado()
        self.create_alarmes()

    def __del__(self):
        self._con.commit()
        self._con.close()

    # Métodos de criação de tabela

    def create_users(self):
        """
        Método responsável por criar a tabela de usuários, caso esta não exista
        """
        try:
            sql_str = f"""
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        created_at TIMESTAMP,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        type SMALLINT NOT NULL
    );
      """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

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
        timestamp TIMESTAMP NOT NULL,
        tensao_fase_a REAL NOT NULL,
        tensao_fase_b REAL NOT NULL,
        tensao_fase_c REAL NOT NULL,
        corrente_fase_a REAL NOT NULL,
        corrente_fase_b REAL NOT NULL,
        corrente_fase_c REAL NOT NULL,
        potencia_ativa_a REAL NOT NULL,
        potencia_ativa_b REAL NOT NULL,
        potencia_ativa_c REAL NOT NULL,
        potencia_ativa_total REAL NOT NULL,
        potencia_reativa_a REAL NOT NULL,
        potencia_reativa_b REAL NOT NULL,
        potencia_reativa_c REAL NOT NULL,
        potencia_reativa_total REAL NOT NULL,
        potencia_aparente_a REAL NOT NULL,
        potencia_aparente_b REAL NOT NULL,
        potencia_aparente_c REAL NOT NULL,
        potencia_aparente_total REAL NOT NULL,
        fator_potencia_a REAL NOT NULL,
        fator_potencia_b REAL NOT NULL,
        fator_potencia_c REAL NOT NULL,
        fator_potencia_total REAL NOT NULL,
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

    def create_alarmes(self):
        """
        Método responsável por criar a tabela que registra os alarmes dos medidores
        """
        try:
            sql_str = f"""
        CREATE TABLE IF NOT EXISTS alarmes (
            id UUID PRIMARY KEY,
            medidor_id SMALLINT NOT NULL,
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

    def add_alarme(self, id, timestamp, message):
        """
        Método responsável por adicionar um novo alarme no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{id}', '{timestamp}', '{message}'"
            sql_str = f"INSERT INTO alarmes (medidor_id, timestamp, message) VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
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
