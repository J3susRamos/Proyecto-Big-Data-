import socket
try:
    s = socket.create_connection(('kafka', 29092), timeout=5)
    print('KAFKA OK - puerto 29092 accesible')
    s.close()
except Exception as e:
    print('KAFKA FALLO:', e)
