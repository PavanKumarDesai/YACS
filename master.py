#MASTER
import json
import socket
import time
import sys
import random
import numpy as np
import logging 
from threading import Thread,Lock,Semaphore
lock=Lock()
sema=Semaphore()
param={} #holds the required info about each worker
map_list=[]
reduce_list=[]
formatter = logging.Formatter('%(asctime)s,%(message)s')
serverName = '127.0.0.1'    
clientSocket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#used for logging
def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file,'a')        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

#connecting to workers
def init_work(param):
    worker1_port = param[1][1]
    worker2_port = param[2][1]
    worker3_port = param[3][1]
    clientSocket1.connect((serverName,worker1_port))
    clientSocket2.connect((serverName,worker2_port))
    clientSocket3.connect((serverName,worker3_port))

#listen updates from workers
def listen_updates(a,b,c):
    host1=''
    port1=5001
    logger1=logging.getLogger("for_plot")
    logger=logging.getLogger("log_from_master")
    s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s1.bind((host1,port1))
    while 1:
        s1.listen(50) #can be increased/decreased depending on no.of updates
        conn,addr=s1.accept()
        #print('connected to this by adress',addr)
        while 1:
            comp=conn.recv(2048)
            if comp:
                comp=comp.decode()
                comp=comp.strip().split('\n')
                
                for i in comp:
                    z,x=i.split(' ')
                    _,g=z.split('_',1)
                    if g[0]=='M':
                        map_list.remove(z)
                    if g[0]=='R':
                        reduce_list.remove(z)
                    logger.info(z)
                    if x==(str(param[1][1])):
                        lock.acquire()
                        param[1][0]+=1
                        lock.release()
                    if x==(str(param[2][1])):
                        lock.acquire()
                        param[2][0]+=1
                        lock.release()
                    if x==(str(param[3][1])):
                        lock.acquire()
                        param[3][0]+=1
                        lock.release()
            else:
                conn.close()
                break
        logger1.info("Worker_1: "+str(a-(param[1][0])))
        logger1.info("Worker_2: "+str(b-(param[2][0])))
        logger1.info("Worker_3: "+str(c-(param[3][0])))

#listen job requests
def listen_requests(scheme):
    logger=logging.getLogger("log_from_master")
    host=''
    port=5000
    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.bind((host,port))
    while 1:
        s.listen(50) #can be increased/decreased as per job requests
        conn,addr=s.accept()
        job_threads=[]
        #print('connected to this by adress',addr)
        while 1:
            jobs=conn.recv(2048)
            if jobs:
                jobs=jobs.decode()
                jobs=json.loads(jobs)
                #print(jobs)
                logger.info(str(jobs['job_id'])+"_Recevied")
                for mt in jobs['map_tasks']:
                    map_list.append(mt['task_id'])
                for mt in jobs['reduce_tasks']:
                    reduce_list.append(mt['task_id'])
                jobs_sch = Thread(target=parsejob_sendworker, args=(jobs,scheme,))
                job_threads.append(jobs_sch)
                jobs_sch.start()
            else:
                conn.close()
                break
        
        for i in job_threads:
            jb=i
            jb.join()
            job_threads.remove(i)
        
#parsing,checking for map completion,scheduling tasks  
def parsejob_sendworker(jobs,scheme):
    logger=logging.getLogger("log_from_master")
    print("Job:"+str(jobs['job_id'])+" Recevied for execution")
    map_tasks_sch=Thread(target=schedule_tasks,args=(jobs,jobs['map_tasks'],scheme,))
    reduce_tasks_sch=Thread(target=schedule_tasks,args=(jobs,jobs['reduce_tasks'],scheme,))
    print("Job:"+str(jobs['job_id'])+" Started")
    map_tasks_sch.start()
    map_tasks_sch.join()
    check=jobs['reduce_tasks'][0]['task_id']
    st,_=check.split('_')
    check1=True
    while(check1):
        check2=0
        for i in map_list:
            if i.startswith(st)==True:
                check2=1
                break
        if check2==0:
            check1=False
        else:
            time.sleep(1)
    reduce_tasks_sch.start()
    reduce_tasks_sch.join()
    
#select worker based on the scheme
def schedule_tasks(jobs,tasks,scheme):
    server_dict={}
    server_dict[1]=clientSocket1
    server_dict[2]=clientSocket2
    server_dict[3]=clientSocket3
    if scheme=="RANDOM":
        tasks_th=[]
        for task in tasks:
            #print(task)
            select_list=[1,2,3]
            select_worker=random.choice(select_list)
            sema.acquire()
            while ((param[1][0]<1) and (param[2][0]<1) and (param[3][0]<1) ):
                time.sleep(1)
            if param[select_worker][0]>0:
                send=Thread(target=final_send,args=(jobs,task,server_dict[select_worker],select_worker,))
                tasks_th.append(send)
                send.start()
                sema.release()
                continue
            select_list.remove(select_worker)
            select_worker1=random.choice(select_list)
            if param[select_worker1][0]>0:
                send=Thread(target=final_send,args=(jobs,task,server_dict[select_worker1],select_worker1,))
                tasks_th.append(send)
                send.start()
                sema.release()
                continue
            select_list.remove(select_worker1)
            select_worker2=random.choice(select_list)
            if param[select_worker2][0]>0:
                send=Thread(target=final_send,args=(jobs,task,server_dict[select_worker2],select_worker2,))
                tasks_th.append(send)
                send.start()
                sema.release()
                continue
            print("Oh NO!")
        for i in tasks_th:
            tk=i
            tk.join()
            tasks_th.remove(i)

    if scheme=="RR":
        tasks_th=[]
        prev_worker=0
        for task in tasks:
            #print(task)
            sema.acquire()
            while ((param[1][0]<1) and (param[2][0]<1) and (param[3][0]<1) ):
                time.sleep(1)
            select_worker=(prev_worker+1)%3
            if select_worker==0:
                select_worker=3
            if param[select_worker][0]<1:
                select_worker=(select_worker+1)%3
                if select_worker==0:
                    select_worker=3
            if param[select_worker][0]<1:
                select_worker=(select_worker+1)%3
                if select_worker==0:
                    select_worker=3
            prev_worker=select_worker
            send=Thread(target=final_send,args=(jobs,task,server_dict[select_worker],select_worker,))
            tasks_th.append(send)
            send.start()
            sema.release()
        for i in tasks_th:
            tk=i
            tk.join()
            tasks_th.remove(i)
            
    if scheme=="LL":
        tasks_th=[]
        use_dict={}
        for task in tasks:
            #print(task)
            sema.acquire()
            while ((param[1][0]<1) and (param[2][0]<1) and (param[3][0]<1) ):
                time.sleep(1)
            from_1=param[1][0]
            use_dict[from_1]=1
            from_2=param[2][0]
            use_dict[from_2]=2
            from_3=param[3][0]
            use_dict[from_3]=3
            max1 = from_3 if from_3 > from_2 else from_2
            max2 = max1 if max1 > from_1 else from_1
            select_worker=use_dict[max2]
            send=Thread(target=final_send,args=(jobs,task,server_dict[select_worker],select_worker,))
            tasks_th.append(send)
            send.start()
            sema.release()
        for i in tasks_th:
            tk=i
            tk.join()
            tasks_th.remove(i)
  
#finally send task to selected worker  
def final_send(jobs,task,clientSocket,select_worker):
    worker_port=param[select_worker][1]
    to_send=task['task_id']+' '+str(task["duration"])+'\n'
    sys.stdout.flush()
    lock.acquire()
    param[select_worker][0]-=1
    lock.release()
    clientSocket.send(to_send.encode())
    sys.stdout.flush()   

if __name__=="__main__":
    path_to_config=sys.argv[1]
    scheme=sys.argv[2]
    f = open (path_to_config, "r") 
    data = json.loads(f.read())
    #print(data,scheme)
    for i in data['workers']:
        key=i['worker_id']
        param[key]=[]
        param[key].append(i['slots'])
        param[key].append(i['port'])
    a=param[1][0]
    b=param[2][0]
    c=param[3][0]
    filen1="log_from_master_"+str(scheme)+".log"
    f2='master_analysis_'+str(scheme)+".log"
    logger = setup_logger('log_from_master', filen1)
    logger1 = setup_logger('for_plot', f2)
    init_work(param)
    incoming_jobs = Thread(target=listen_requests, args=(scheme,)) 
    updates=Thread(target=listen_updates,args=(a,b,c,))
    incoming_jobs.start()
    updates.start()
    incoming_jobs.join()
    updates.join()
    clientSocket1.close()
    clientSocket2.close()
    clientSocket3.close()