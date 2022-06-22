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

        now = datetime.now()
        minute = now.minute
        if(minute == 0 or minute == 15 or minute == 30 or minute == 45):
            # Cria uma lista com todos os medidores cadastrados
            database_meds = db.get_all_medidores()
            medidores = medidores_factory(database_meds, db)
            collect_threads = []
            recover_threads = []
            for med in medidores:
                collect_threads.append(Thread(target=med.collect))
                recover_threads.append(Thread(target=med.recover))

            for collect_thread in collect_threads:
                collect_thread.start()

            for collect_thread in collect_threads:
                collect_thread.join()

            for recover_thread in recover_threads:
                recover_thread.start()

            sleep(800)
