import multiprocessing
import sys
import Exercicio_3_dht as dht

# Tentativa de automatizacao da criacao da dht
# Atualmente nao funciona

process_number = 8

jobs = []

# Cria processos que dividem a dht entre eles
for i in range(0, process_number):
    process_dht = multiprocessing.Process(target=dht.run())
    jobs.append(process_dht)

# Inicia todos os jobs
for j in jobs:
    j.start()

# Assegura que todas os jobs terminaram
for j in jobs:
    j.join()