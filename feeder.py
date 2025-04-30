import threading
import queue
import time

# Fila para troca de mensagens entre threads
q = queue.Queue()

def produtor():
    for i in range(5):
        print(f"Produtor: produzindo {i}")
        q.put(i)
        time.sleep(1)
    q.put(None)  # Sinaliza fim

def consumidor():
    while True:
        item = q.get()
        if item is None:
            break  # Sai se receber sinal de fim
        print(f"Consumidor: recebeu {item}")
        q.task_done()

# Criando as threads
t1 = threading.Thread(target=produtor)
t2 = threading.Thread(target=consumidor)

# Iniciando as threads
t1.start()
t2.start()

# Esperando terminar
t1.join()
t2.join()
