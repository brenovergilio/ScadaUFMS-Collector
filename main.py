from MedidorMD30 import MedidorMD30
from dbhandler import DBHandler
from threading import Thread
from time import sleep

medidores = []

def medidores_factory(meds, dbhandler):
  for med in meds:
    # med[0] é IP, med[1] é a data de criação (não utilizado aqui), med[2] é o nome e med[3] é a porta de conexão
    medidor = MedidorMD30(med[0], med[2], dbhandler=dbhandler)
    medidores.append(medidor)

if __name__ == '__main__':
  print("ScadaUFMS-Collector started")
  db = DBHandler('..\\ScadaUFMS.db')
  while(True):
    medidores_factory(db.get_all_med(), db)
    try:
      for med in medidores:
        Thread(target=med.populate()).start()
      sleep(15)
      medidores = []
    except Exception as e:
      print('Erro: ', e.args)