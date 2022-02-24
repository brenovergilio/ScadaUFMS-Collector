import sqlite3
import datetime
from threading import Lock

class DBHandler():
  """
  Classe para a manipulação do Database
  """

  def __init__(self, dbpath):
    self._dbpath = dbpath
    self._con = sqlite3.connect(self._dbpath, check_same_thread=False)
    self._cursor = self._con.cursor()
    self._lock = Lock()
    self.create_medidores()
    self.create_datatable()
    #self.create_alarmes()

  def __del__(self):
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
        timestamp TEXT NOT NULL,
        nome TEXT NOT NULL,
        porta INTEGER NOT NULL
      );
      """
      self._lock.acquire()
      self._cursor.execute(sql_str)
      self._con.commit()
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release()
    
  def create_datatable(self):
    """
    Método responsável por criar a tabela que armazena os dados coletados pelos medidores
    """
    try:
      sql_str = f"""
      CREATE TABLE IF NOT EXISTS medicoes (
        medidor TEXT NOT NULL,
        timestamp TEXT NOT NULL,
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
        fator_potencia_a REAL NOT NULL,
        fator_potencia_b REAL NOT NULL,
        fator_potencia_c REAL NOT NULL,
        fator_potencia_total REAL NOT NULL,
        FOREIGN KEY (medidor) REFERENCES medidores(ip)
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
      ---
      );
      """
      self._lock.acquire()
      self._cursor.execute(sql_str)
      self._con.commit()
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release() 
  
  #Métodos de inserção

  def add_medidor(self, ip, nome, porta):
    """
    Método responsável por adicionar um novo medidor no Database
    """
    try:
      self._lock.acquire()
      timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')
      str_values = f'"{ip}", "{timestamp}", "{nome}", {porta}'
      sql_str = f'INSERT INTO medidores VALUES ({str_values});'
      self._cursor.execute(sql_str)
      self._con.commit()
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release()

  def add_data(self, ip, data):
    """
    Método responsável por adicionar as medições do medidor no Database
    """
    try:
      self._lock.acquire()
      timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')
      str_values = f'"{ip}", "{timestamp}",' + ','.join(str(value) for value in data)
      sql_str = f'INSERT INTO medicoes VALUES ({str_values});'
      self._cursor.execute(sql_str)
      self._con.commit()
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release()
  
  def add_alarme(self, data):
    """
    Método responsável por adicionar um novo alarme no Database
    """
    try:
      self._lock.acquire()
      timestamp = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S.%f')
      str_values = f"'{timestamp}', " + ','.join(data)
      sql_str = f'INSERT INTO alarmes VALUES ({str_values});'
      self._cursor.execute(sql_str)
      self._con.commit()
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release()
  
  #Métodos de remoção
  
  def del_medidor(self, ip):
    pass
  
  def del_data(self, ip):
    pass

  def del_alarme(self, id):
    pass

  #Métodos de seleção

  def get_all_med(self):
    """
    Retorna todos os medidores cadastrados
    """
    try:
      self._lock.acquire()
      sql_str = "SELECT * FROM medidores;"
      medidores = self._cursor.execute(sql_str).fetchall()
      self._con.commit()
      return medidores
    except Exception as e:
      print('Erro: ', e.args)
    finally:
      self._lock.release()
