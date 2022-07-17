from pyModbusTCP.client import ModbusClient
from datetime import datetime, timedelta
import time
import numpy as np
import uuid

SECONDS_IN_A_DAY = 86_400
SECONDS_IN_AN_HOUR = 3_600
BRASILIA_STANDARD_TIME = 10_800
SECONDS_IN_30_MINUTES = 1_800
SECONDS_IN_15_MINUTES = 900


class MedidorMD30():
    """
    Classe que representa o Medidor MD30
    """

    def __init__(self, id, ip, nome, dbhandler, porta=1001):
        self._id = id
        self._ip = ip
        self._nome = nome
        self._porta = porta
        self._dbhandler = dbhandler
        self._client = self.set_client()

    def convert_to_float(self, value):
        value.reverse()
        retorno = np.array(value, dtype=np.int16).copy().view('float32')
        return float(retorno)

    def set_client(self):
        client = ModbusClient(self._ip, self._porta, unit_id=1)
        return client

    def split_int_64_into_int16_array(self, unix_timestamp):
        first_piece = (unix_timestamp >> 48)
        second_piece = (unix_timestamp >> 32)
        third_piece = (unix_timestamp >> 16)
        fourth_piece = (unix_timestamp & 0xffff)
        return [first_piece, second_piece, third_piece, fourth_piece]

    def convert_int16_to_int64(self, array_of_int_16):
        return ((array_of_int_16[0] << 48) | (array_of_int_16[1] << 32) | (array_of_int_16[2] << 16) | (array_of_int_16[3] & 0xffff))

    def bitfield(self, n):
        bits = [int(digit) for digit in bin(n)[2:]]
        while(len(bits) < 16):
            bits.insert(0, 0)
        return bits

    def read_MM(self, unix_timestamp_to_read):
        pieces_of_unix_timestamp = self.split_int_64_into_int16_array(
            unix_timestamp_to_read)
        wrote = self._client.write_multiple_registers(
            160, pieces_of_unix_timestamp)

        if(wrote):
            result = self._client.read_holding_registers(200, 22)
            timestamp = datetime.utcfromtimestamp(self.convert_int16_to_int64(
                result[0:4]) - SECONDS_IN_A_DAY - SECONDS_IN_AN_HOUR).strftime('%Y-%m-%d %H:%M:%S')
            tensao_fase_a = self.convert_to_float(value=result[4:6])
            tensao_fase_b = self.convert_to_float(value=result[6:8])
            tensao_fase_c = self.convert_to_float(value=result[8:10])
            corrente_fase_a = self.convert_to_float(
                value=result[10:12])
            corrente_fase_b = self.convert_to_float(
                value=result[12:14])
            corrente_fase_c = self.convert_to_float(
                value=result[14:16])
            potencia_ativa_total = self.convert_to_float(
                value=result[16:18])
            potencia_reativa_total = self.convert_to_float(
                value=result[18:20])
            fator_de_potencia = self.convert_to_float(
                value=result[20:22])
            medicoes = [tensao_fase_a, tensao_fase_b, tensao_fase_c, corrente_fase_a,
                        corrente_fase_b, corrente_fase_c, potencia_ativa_total, potencia_reativa_total, fator_de_potencia]
            self._dbhandler.add_medicoes(self._id, timestamp, medicoes)
            return True

        return False

    def collect(self):
        try:
            unix_timestamp_to_read = int(
                time.time()) + SECONDS_IN_A_DAY - BRASILIA_STANDARD_TIME - SECONDS_IN_30_MINUTES
            related_time = (
                datetime.now() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
            if(self._client.open()):
                print(
                    f"[Collecting data from {self._ip} - {self._nome} on port {self._porta} related to {related_time}]")
                success = self.read_MM(unix_timestamp_to_read)
                if(not success):
                    self._dbhandler.add_missing_medicao_md30(
                        self._id, unix_timestamp_to_read)
            else:
                print(
                    f'[Adding alarm to {self._ip} - {self._nome} related to {related_time}]')
                self._dbhandler.add_alarme(
                    str(uuid.uuid4()), self._id, str(related_time), 'Perda de conexão')
                self._dbhandler.add_missing_medicao_md30(
                    self._id, unix_timestamp_to_read)
        except Exception as e:
            print(f"Erro {e.args} em {self._nome}")
        finally:
            self._client.close()

    def recover(self):
        missing_medicoes_md30 = self._dbhandler.get_all_missing_medicoes_md30(
            self._id)

        self._client.open()

        for missing in missing_medicoes_md30:
            unix_timestamp = missing[1]
            if(self.read_MM(unix_timestamp) and self._dbhandler.delete_missing_medicao_md30(self._id, unix_timestamp)):
                self._dbhandler.delete_missing_medicao_md30(
                    self._id, unix_timestamp)

        self._client.close()

    # def collect(self):
    #     try:
    #         medicoes = []
    #         related_time = datetime.now()
    #         if(self._client.open()):
    #             print(
    #                 f"[COLLECTING DATA FROM {self._ip} ON PORT {self._porta}] at {now}")

    #             medicoes.append(self.tensao_fase_a())
    #             medicoes.append(self.tensao_fase_b())
    #             medicoes.append(self.tensao_fase_c())
    #             medicoes.append(self.corrente_fase_a())
    #             medicoes.append(self.corrente_fase_b())
    #             medicoes.append(self.corrente_fase_c())
    #             medicoes.append(self.potencia_ativa_a())
    #             medicoes.append(self.potencia_ativa_b())
    #             medicoes.append(self.potencia_ativa_c())
    #             medicoes.append(self.potencia_ativa_total())
    #             medicoes.append(self.potencia_reativa_a())
    #             medicoes.append(self.potencia_reativa_b())
    #             medicoes.append(self.potencia_reativa_c())
    #             medicoes.append(self.potencia_reativa_total())
    #             medicoes.append(self.potencia_aparente_a())
    #             medicoes.append(self.potencia_aparente_b())
    #             medicoes.append(self.potencia_aparente_c())
    #             medicoes.append(self.potencia_aparente_total())
    #             medicoes.append(self.fator_potencia_a())
    #             medicoes.append(self.fator_potencia_b())
    #             medicoes.append(self.fator_potencia_c())
    #             medicoes.append(self.fator_potencia_total())

    #             self._dbhandler.add_medicoes(self._id, str(now), medicoes)
    #         else:
    #             print(f'[ADDING ALARM TO {self._ip} at {now}]')
    #             self._dbhandler.add_alarme(
    #                 str(uuid.uuid4()), self._id, str(now), 'Perda de conexão')
    #     except Exception as e:
    #         print("Erro: ", e.args)
    #     finally:
    #         self._client.close()

    # def read_MM_entire_month(self):
    #     try:
    #         current_unix_timestamp = int(
    #             time.time()) + SECONDS_IN_A_DAY - CAMPO_GRANDE_STANDARD_TIME
    #         entire_month = datetime.today().replace(
    #             day=1, hour=0, minute=0, second=0, microsecond=0)
    #         unix_timestamp_entire_month = int(entire_month.strftime(
    #             "%s")) + SECONDS_IN_A_DAY - CAMPO_GRANDE_STANDARD_TIME
    #         self._client.open()
    #         while(current_unix_timestamp - unix_timestamp_entire_month > 1800):
    #             pieces_of_unix_timestamp = self.split_int_64_into_int16_array(
    #                 unix_timestamp_entire_month)
    #             wrote = self._client.write_multiple_registers(
    #                 160, pieces_of_unix_timestamp)
    #             # while(not wrote):
    #             #     wrote = self._client.write_multiple_registers(
    #             #         160, pieces_of_unix_timestamp)
    #             if(wrote):
    #                 result = self._client.read_holding_registers(200, 20)
    #                 timestamp = datetime.utcfromtimestamp(self.convert_int16_to_int64(
    #                     result[0:4]) - SECONDS_IN_A_DAY).strftime('%Y-%m-%d %H:%M:%S')
    #                 tensao_fase_a = self.convert_to_float(value=result[4:6])
    #                 tensao_fase_b = self.convert_to_float(value=result[6:8])
    #                 tensao_fase_c = self.convert_to_float(value=result[8:10])
    #                 corrente_fase_a = self.convert_to_float(
    #                     value=result[10:12])
    #                 corrente_fase_b = self.convert_to_float(
    #                     value=result[12:14])
    #                 corrente_fase_c = self.convert_to_float(
    #                     value=result[14:16])
    #                 potencia_ativa_total = self.convert_to_float(
    #                     value=result[16:18])
    #                 potencia_reativa_total = self.convert_to_float(
    #                     value=result[18:20])
    #                 print("===========================================================")
    #                 print(
    #                     f"Hora: {timestamp}")
    #                 print(
    #                     f"Tensão Fase A: {tensao_fase_a}")
    #                 print(
    #                     f"Tensão Fase B: {tensao_fase_b}")
    #                 print(
    #                     f"Tensão Fase C: {tensao_fase_c}")
    #                 print(
    #                     f"Corrente Fase A: {corrente_fase_a}")
    #                 print(
    #                     f"Corrente Fase B: {corrente_fase_b}")
    #                 print(
    #                     f"Corrente Fase C: {corrente_fase_c}")
    #                 print(
    #                     f"Potencia Ativa Total: {potencia_ativa_total}")
    #                 print(
    #                     f"Potencia Reativa Total: {potencia_reativa_total}")
    #                 medicoes = [tensao_fase_a, tensao_fase_b, tensao_fase_c, corrente_fase_a,
    #                             corrente_fase_b, corrente_fase_c, potencia_ativa_total, potencia_reativa_total]
    #                 self._dbhandler.add_medicoes(self._id, timestamp, medicoes)
    #             else:
    #                 print("did not wrote")
    #                 raise Exception()
    #             unix_timestamp_entire_month += SECONDS_IN_15_MINUTES
    #             time.sleep(0.5)
    #         self._client.close()
    #     except Exception as e:
    #         print(
    #             f"hora de leitura atual -> {unix_timestamp_entire_month} | hora corrente -> {current_unix_timestamp}")
    #         print(e)
    #         self._client.close()
    #         exit(1)

    # Função invocada para realizar todas as requisições aos medidores MD30

    # def request(self, registrador, quantidade_regs, is_short=False):
    #     try:
    #         value = self._client.read_holding_registers(
    #             registrador, quantidade_regs)
    #         return value if is_short else self.convert_to_float(value)
    #     except Exception as e:
    #         print("Erro: ", e.args)

    # def tensao_fase_a(self):
    #     return self.request(68, 2)

    # def tensao_fase_b(self):
    #     return self.request(70, 2)

    # def tensao_fase_c(self):
    #     return self.request(72, 2)

    # def corrente_fase_a(self):
    #     return self.request(74, 2)

    # def corrente_fase_b(self):
    #     return self.request(76, 2)

    # def corrente_fase_c(self):
    #     return self.request(78, 2)

    # def potencia_ativa_a(self):
    #     return self.request(80, 2)

    # def potencia_ativa_b(self):
    #     return self.request(82, 2)

    # def potencia_ativa_c(self):
    #     return self.request(84, 2)

    # def potencia_ativa_total(self):
    #     return self.request(86, 2)

    # def potencia_reativa_a(self):
    #     return self.request(88, 2)

    # def potencia_reativa_b(self):
    #     return self.request(90, 2)

    # def potencia_reativa_c(self):
    #     return self.request(92, 2)

    # def potencia_reativa_total(self):
    #     return self.request(94, 2)

    # def potencia_aparente_a(self):
    #     return self.request(96, 2)

    # def potencia_aparente_b(self):
    #     return self.request(98, 2)

    # def potencia_aparente_c(self):
    #     return self.request(100, 2)

    # def potencia_aparente_total(self):
    #     return self.request(102, 2)

    # def fator_potencia_a(self):
    #     return self.request(104, 2)

    # def fator_potencia_b(self):
    #     return self.request(106, 2)

    # def fator_potencia_c(self):
    #     return self.request(108, 2)

    # def fator_potencia_total(self):
    #     return self.request(110, 2)
