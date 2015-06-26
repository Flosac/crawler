import json
from multiprocessing import Process
import os
import socket
from threading import Thread
from time import sleep, time
import sys

__author__ = 'florian'

QUEUE = []
RUNNING = True
def writing():
    global QUEUE, RUNNING
    while RUNNING:
        print 'Queue has items: ' + str(len(QUEUE))
        if len(QUEUE) > 0:
            while len(QUEUE)>0:
                item = QUEUE.pop(0).strip()
                if item == "":
                    continue
                try:
                    msg = json.loads(item)
                    f = open(msg['file'], 'a+')
                    for line in msg['data']:
                        f.write(line.encode('utf8').strip() + "\n")
                    f.close()
                    del f
                except Exception as e:
                    print e, item
        else:
            sleep(5)

def talking(client, address):
    global QUEUE
    line = ''
    #client.send('Hello Ure address is {0}\n'.format(address))
    lastmsg = time()
    while line != ':quit':
        if lastmsg - time() > 1800: break
        data = client.recv(512*1024)
        data = str(data).strip()
        for line in data.split('\n'):
            if line == "": continue
            lastmsg = time()
            if line == ':quit':
                break
            elif line == ':flush':
                client.send(json.dumps(QUEUE))
            else:
                QUEUE.append(line)
        sleep(1)
    client.close()
    return True
scriptpath = os.path.dirname(os.path.realpath(__file__))
socketPath = scriptpath +"/../socket007"
s = socket.socket(socket.AF_UNIX)
s.bind(socketPath)

try:
    wt = Thread(target=writing)
    wt.start()
    while True:
        s.listen(5)
        client, address = s.accept()
        t = Thread(target=talking, args=(client, address))
        t.start()
except (KeyboardInterrupt, SystemExit):
    RUNNING = False
    s.close()
except Exception as e:
    RUNNING = False
    s.close()
    print e
exit()
