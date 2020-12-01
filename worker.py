import socket
import json
import time
import sys
import random
import numpy as np
import threading
import logging 

#intializing the logging and setting up the logger 
def init_log(id,port):
    filen1="Worker_"+str(id)+".log"
    filen2="log_from_worker"
    logging.basicConfig(filename=filen1, format='%(asctime)s,%(message)s', filemode='a') 
    logger=logging.getLogger(filen2) 
    logger.setLevel(logging.DEBUG)
 
#listen to master for tasks 
def listen_master(port):
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host=''
    s.bind((host,port))
    while 1:
        s.listen(50) #can be increased based on tasks receiving
        conn,addr=s.accept()
        w_th=[]
        #print('connected to this by adress',addr)
        while 1:
            task_exec=conn.recv(2048)
            if task_exec:
                task_exec=task_exec.decode()
                duras=task_exec.strip().split('\n')
                print("Recieved Tasks form master: ",(duras))
                copy_duras=duras.copy()
                pol=threading.Thread(target=pool,args=(copy_duras,port,))
                w_th.append(pol)
                pol.start()
            else:
                break
        conn.close()
        '''
        for i in w_th:
            wt=i
            wt.join()
            w_th.remove(i)
        '''
                
#parsing and adding task to the pool
def pool(copy_duras,port):
    dura_th=[]
    logger=logging.getLogger("log_from_worker")
    for dura in copy_duras:
        task,dura1=dura.split(' ')
        logger.info("Task_starting_of_duration: "+str(dura1))
        print("Task added to pool of duration: ",dura1)
        task_sch=threading.Thread(target=add_to_pool,args=(dura,port,))
        dura_th.append(task_sch)
        task_sch.start()
    '''
    for i in dura_th:
            da=i
            da.join()
            dura_th.remove(i)
    '''
    
#executing the task and reporting to master    
def add_to_pool(dura,port):
    task1,dura2=dura.split(' ')
    logger=logging.getLogger("log_from_worker")
    time.sleep(int(dura2))
    logger.info("Task_finished_of_duration: "+str(dura2))
    print("Task finished of duration: "+str(dura2))
    host_to_send = '127.0.0.1'
    port_to_send = 5001
    workerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    workerSocket.connect((host_to_send,port_to_send))
    done=task1+' '+str(port)+'\n'
    workerSocket.send(done.encode())
    workerSocket.close()
    

if __name__=="__main__":
    port=int(sys.argv[1])
    id=int(sys.argv[2])
    init_log(id,port)
    lis_master=threading.Thread(target=listen_master,args=(port,))
    lis_master.start()
    lis_master.join()