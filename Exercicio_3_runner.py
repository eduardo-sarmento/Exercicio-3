import multiprocessing
import sys
import Exercicio_3_sub as sub
"""
def main():
    process_number = 2

    jobs = []

    # Cria processos e divide a quantidade m de requisicoes entre eles 
    for i in range(0, process_number):
        process_client = multiprocessing.Process(target=sub.run())
        jobs.append(process_client)

    # Inicia todos os jobs
    for j in jobs:
        j.start()

    # Assegura que todas os jobs terminaram
    for j in jobs:
        j.join()

if __name__ == "__main__":
    main()
"""