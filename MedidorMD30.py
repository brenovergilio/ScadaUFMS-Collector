from pyModbusTCP.client import ModbusClient
from datetime import datetime, timedelta
import time
import numpy as np
import uuid

SECONDS_IN_A_DAY = 86_400
SECONDS_IN_AN_HOUR = 3_600
SECONDS_IN_30_MINUTES = 1_800


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
            unix_timestamp_to_read = self.generate_timestamp_to_read()
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

    def generate_timestamp_to_read(self):
        offset = time.timezone if (
            time.localtime().tm_isdst == 0) else time.altzone
        offset = int(offset / 60 / 60 * -1) * 3600
        timestamp_to_read = int(
            time.time()) + SECONDS_IN_A_DAY + offset - SECONDS_IN_30_MINUTES + SECONDS_IN_AN_HOUR
        return timestamp_to_read
