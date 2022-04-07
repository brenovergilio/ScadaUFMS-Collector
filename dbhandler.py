import psycopg2 as db
from datetime import date, datetime
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
        self._cursor.execute("SET TIMEZONE='Brazil/West';")
        self._lock = Lock()
        self.create_medidores()
        self.create_medicoes()
        self.create_demandas()
        self.create_feriado()
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
      CREATE TABLE IF NOT EXISTS medidores (
        ip TEXT NOT NULL PRIMARY KEY,
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
      CREATE TABLE IF NOT EXISTS medicoes (
        medidor_ip TEXT NOT NULL,
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
        FOREIGN KEY (medidor_ip) REFERENCES medidores(ip) ON DELETE CASCADE ON UPDATE CASCADE
      );
      """
            self._lock.acquire()
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def create_demandas(self):
        """
        Método responsável por criar a tabela que armazena as demandas nos horários de ponta e fora de ponta
        """
        try:
            sql_str = """
            CREATE TABLE IF NOT EXISTS demandas_diarias (
              medidor_ip TEXT NOT NULL,
              timestamps TIMESTAMP NOT NULL,
              demanda_fora_ponta REAL NOT NULL,
              demanda_ponta REAL NOT NULL,
              FOREIGN KEY (medidor_ip) REFERENCES medidores(ip) ON DELETE CASCADE ON UPDATE CASCADE
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
            id SMALLSERIAL PRIMARY KEY,
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
            medidor_ip TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            message TEXT NOT NULL,
            FOREIGN KEY (medidor_ip) REFERENCES medidores(ip) ON DELETE CASCADE ON UPDATE CASCADE
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

    def add_medidor(self, ip, nome, porta, hora_ponta=17, minuto_ponta=30, intervalo_ponta=3):
        """
        Método responsável por adicionar um novo medidor no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{ip}', NOW(), '{nome}', {porta}, {hora_ponta}, {minuto_ponta}, {intervalo_ponta}"
            sql_str = f"INSERT INTO medidores VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_medicoes(self, ip, timestamp, medicoes):
        """
        Método responsável por adicionar as medições do medidor no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{ip}', '{timestamp}', " + ','.join((str(v)
                                                                for v in medicoes))
            sql_str = f"INSERT INTO medicoes VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_demanda(self, ip, hora_ponta=17, min_ponta=30, intervalo_ponta=3):
        """
        Método responsável por adicionar as demandas ao banco de dados
        """
        try:
            current_date = str(date.today())
            horario_ponta = current_date + f' {hora_ponta}:{min_ponta}:00'
            fim_horario_ponta = current_date + \
                f' {hora_ponta+intervalo_ponta}:{min_ponta}:00'
            self._lock.acquire()
            demanda_fora_ponta = f"SELECT MAX(pot_at_total) FROM (SELECT to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) AS interval, AVG(potencia_ativa_total) as pot_at_total FROM medicoes WHERE medidor='{ip}' AND ((to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) >= '{current_date} 00:00:00' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) < '{horario_ponta}') OR (to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) >= '{fim_horario_ponta}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) <= '{current_date} 23:59:59')) GROUP BY interval ORDER BY interval) as medias;"
            demanda_ponta = f"SELECT MAX(pot_at_total) FROM (SELECT to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) AS interval, AVG(potencia_ativa_total) as pot_at_total FROM medicoes WHERE medidor='{ip}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) >= '{horario_ponta}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / 900) * 900) < '{fim_horario_ponta}' GROUP BY interval ORDER BY interval) as medias;"
            self._cursor.execute(demanda_fora_ponta)
            demanda_fora_ponta = self._cursor.fetchone()[0]
            self._cursor.execute(demanda_ponta)
            demanda_ponta = self._cursor.fetchone()[0]
            if not demanda_fora_ponta:
                demanda_fora_ponta = 0.0
            if not demanda_ponta:
                demanda_ponta = 0.0
            str_values = f"'{ip}', CURRENT_DATE, {demanda_fora_ponta}, {demanda_ponta}"
            sql_str = f"INSERT INTO demandas VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_feriado(self, nome, data):
        """
        Método responsável por adicionar um novo feriado no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{nome}', '{data}'"
            sql_str = f"INSERT INTO feriados (nome, dia) VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def add_alarme(self, ip, timestamp, message):
        """
        Método responsável por adicionar um novo alarme no banco de dados
        """
        try:
            self._lock.acquire()
            str_values = f"'{ip}', '{timestamp}', '{message}'"
            sql_str = f"INSERT INTO alarmes VALUES ({str_values});"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    # Métodos de remoção

    def del_medidor(self, ip):
        """
        Método responsável por deletar um medidor do banco de dados
        """
        try:
            self._lock.acquire()
            sql_str = f"DELETE FROM medidores WHERE ip='{ip}';"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def del_medicoes(self, ip):
        pass

    def del_feriado(self, id):
        """
        Método responsável por deletar um feriado do banco de dados
        """
        try:
            self._lock.acquire()
            sql_str = f"DELETE FROM feriados WHERE id={id};"
            self._cursor.execute(sql_str)
            self._con.commit()
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def del_alarme(self, ip, timestamp):
        """
        Método responsável por deletar um alarme do banco de dados
        """
        try:
            self._lock.acquire()
            sql_str = f"DELETE FROM alarmes WHERE medidor='{ip}' AND timestamp='{timestamp}';"
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
            sql_str = "SELECT * FROM medidores;"
            self._cursor.execute(sql_str)
            medidores = self._cursor.fetchall()
            self._con.commit()
            return medidores
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def get_all_feriados(self):
        """
        Retorna todos os feriados cadastrados
        """
        try:
            self._lock.acquire()
            sql_str = "SELECT * FROM feriados order by dia desc;"
            self._cursor.execute(sql_str)
            medidores = self._cursor.fetchall()
            self._con.commit()
            return medidores
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def get_alarme_per_medidor(self, ip):
        """
        Retorna todos os medidores cadastrados
        """
        try:
            self._lock.acquire()
            sql_str = f"SELECT * FROM alarmes where medidor='{ip}' order by timestamp desc;"
            self._cursor.execute(sql_str)
            alarmes = self._cursor.fetchall()
            self._con.commit()
            return alarmes
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._lock.release()

    def get_all_medicoes_per_period(self, ip, initial_datetime, final_datetime):
        """
        Retorna todas as medições de um determinado intervalo de dias 
        """
        try:
            self._lock.acquire()
            sql_str = f"SELECT * FROM medicoes WHERE medidor = '{ip}' AND timestamp >= '{initial_datetime}'  AND timestamp <= '{final_datetime}';"
            self._cursor.execute(sql_str)
            medicoes = self._cursor.fetchall()
            self._con.commit()
            return medicoes
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def get_medias_per_interval(self, ip, interval, initial_datetime, final_datetime):
        """
        Retorna todas as médias das potências ativas, reativas e aparentes em um determinado intervalo de tempo (em segundos)
        """
        try:
            self._lock.acquire()
            sql_str = f"SELECT to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) AS interval, AVG(potencia_ativa_total), AVG(potencia_reativa_total), AVG(potencia_aparente_total) FROM medicoes WHERE medidor='{ip}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) >= '{initial_datetime}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) <= '{final_datetime}' GROUP BY interval ORDER BY interval;"
            self._cursor.execute(sql_str)
            medicoes = self._cursor.fetchall()
            self._con.commit()
            return medicoes
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()

    def get_max_per_interval(self, ip, interval, initial_datetime, final_datetime):
        """
        Retorna todas as máximas potências ativas, reativas e aparentes em um determinado intervalo de tempo (em segundos)
        """
        try:
            self._lock.acquire()
            sql_str = f"SELECT to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) AS interval, MAX(potencia_ativa_total), MAX(potencia_reativa_total), MAX(potencia_aparente_total) FROM medicoes WHERE medidor='{ip}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) >= '{initial_datetime}' AND to_timestamp(FLOOR(extract('epoch' FROM timestamp::timestamptz) / {interval}) * {interval}) <= '{final_datetime}' GROUP BY interval ORDER BY interval;"
            self._cursor.execute(sql_str)
            medicoes = self._cursor.fetchall()
            self._con.commit()
            return medicoes
        except Exception as e:
            print('Erro: ', e.args)
        finally:
            self._lock.release()
