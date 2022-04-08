from pyModbusTCP.client import ModbusClient
from datetime import datetime
import numpy as np


class MedidorMD30():
    """
    Classe que representa o Medidor MD30 e invoca suas relações com o Database
    """

    def __init__(self, id, ip, nome, dbhandler, porta=1001):
        self._id = id
        self._ip = ip
        self._nome = nome
        self._porta = porta
        self._dbhandler = dbhandler
        self._client = self.set_client()

    # Função invocada para realizar todas as requisições aos medidores MD30

    def request(self, registrador, quantidade_regs, is_short=False):
        try:
            value = self._client.read_holding_registers(
                registrador, quantidade_regs)
            if not value:
                value = 0.0
            return value if is_short else self.convert_to_float(value)
        except Exception as e:
            print("Erro: ", e.args)

    def tensao_fase_a(self):
        return self.request(68, 2)

    def tensao_fase_b(self):
        return self.request(70, 2)

    def tensao_fase_c(self):
        return self.request(72, 2)

    def corrente_fase_a(self):
        return self.request(74, 2)

    def corrente_fase_b(self):
        return self.request(76, 2)

    def corrente_fase_c(self):
        return self.request(78, 2)

    def potencia_ativa_a(self):
        return self.request(80, 2)

    def potencia_ativa_b(self):
        return self.request(82, 2)

    def potencia_ativa_c(self):
        return self.request(84, 2)

    def potencia_ativa_total(self):
        return self.request(86, 2)

    def potencia_reativa_a(self):
        return self.request(88, 2)

    def potencia_reativa_b(self):
        return self.request(90, 2)

    def potencia_reativa_c(self):
        return self.request(92, 2)

    def potencia_reativa_total(self):
        return self.request(94, 2)

    def potencia_aparente_a(self):
        return self.request(96, 2)

    def potencia_aparente_b(self):
        return self.request(98, 2)

    def potencia_aparente_c(self):
        return self.request(100, 2)

    def potencia_aparente_total(self):
        return self.request(102, 2)

    def fator_potencia_a(self):
        return self.request(104, 2)

    def fator_potencia_b(self):
        return self.request(106, 2)

    def fator_potencia_c(self):
        return self.request(108, 2)

    def fator_potencia_total(self):
        return self.request(110, 2)

    # Conversão de valores retornados pelo medidor para float
    def convert_to_float(self, value):
        retorno = np.array(value, dtype=np.int16).view('float32')
        return float(retorno)

    # Configurar ip e porta do client MODBUS
    def set_client(self):
        client = ModbusClient()
        client.host(self._ip)
        client.port(self._porta)
        return client

    # Requista dados ao medidor e armazena no banco de dados
    def collect(self):
        try:
            medicoes = []
            self._client.open()
            if(self._client.is_open()):
                now = datetime.now()
                print(
                    f"[COLLECTING DATA FROM {self._ip} ON PORT {self._porta}] at {now}")

                medicoes.append(self.tensao_fase_a())
                medicoes.append(self.tensao_fase_b())
                medicoes.append(self.tensao_fase_c())
                medicoes.append(self.corrente_fase_a())
                medicoes.append(self.corrente_fase_b())
                medicoes.append(self.corrente_fase_c())
                medicoes.append(self.potencia_ativa_a())
                medicoes.append(self.potencia_ativa_b())
                medicoes.append(self.potencia_ativa_c())
                medicoes.append(self.potencia_ativa_total())
                medicoes.append(self.potencia_reativa_a())
                medicoes.append(self.potencia_reativa_b())
                medicoes.append(self.potencia_reativa_c())
                medicoes.append(self.potencia_reativa_total())
                medicoes.append(self.potencia_aparente_a())
                medicoes.append(self.potencia_aparente_b())
                medicoes.append(self.potencia_aparente_c())
                medicoes.append(self.potencia_aparente_total())
                medicoes.append(self.fator_potencia_a())
                medicoes.append(self.fator_potencia_b())
                medicoes.append(self.fator_potencia_c())
                medicoes.append(self.fator_potencia_total())

                self._dbhandler.add_medicoes(self._id, str(now), medicoes)
            else:
                self._dbhandler.add_alarme(
                    self._id, str(now), 'Perda de conexão')
        except Exception as e:
            print("Erro: ", e.args)
        finally:
            self._client.close()
