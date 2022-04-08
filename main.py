from time import sleep
from MedidorMD30 import MedidorMD30
from dbhandler import DBHandler
from threading import Thread
from datetime import datetime


def medidores_factory(meds, db):
    medidores = []
    for med in meds:
        medidor = MedidorMD30(med[0], med[1], med[3], db)
        medidores.append(medidor)
    return medidores


if __name__ == '__main__':
    print("ScadaUFMS-Collector started")
    db = DBHandler()

    while(True):
        try:
            # Aparentemente mais preciso que sleep(30) - testar mais tarde novamente
            now = datetime.now()
            second = now.second
            if(second == 0 or second == 30):
                # Cria uma lista com todos os medidores cadastrados
                database_meds = db.get_all_medidores()
                medidores = medidores_factory(database_meds, db)
                for med in medidores:
                    Thread(target=med.collect()).start()
                sleep(1.5)
                hour = now.hour
                minute = now.minute
                if(hour == 23 and minute == 59 and second == 30):
                    for med in medidores:
                        Thread(target=db.add_demanda,
                               args=med._ip).start()
                medidores = []
        except Exception as e:
            print('Erro: ', e.args)
