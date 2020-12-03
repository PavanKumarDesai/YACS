#analysis
import sys
import matplotlib.pyplot as plt 
from datetime import datetime
import statistics
import plotly.graph_objects as go

#To find mean and median of task completion time
def worker_analysis(lines):
    starting=[]
    finished=[]
    diff=[]
    diff1=[]
    for line in lines:
        if not line:
            continue
        #print(line)
        a,b=line.strip().rsplit(',',1)
        _,q=a.split(' ')
        q=q.replace(',','.')
        c,dura=b.split(':')
        c=c.strip()
        dura=int(dura.strip())
        if c=="Task_starting_of_duration":
            tup=(q,dura)
            starting.append(tup)
        else:
            tup=(q,dura)
            finished.append(tup)
    for dt,dura in starting:
        for dt1,dura1 in finished:
            if dura==dura1:
                sim=dt1
                tup1=(dt,dt1)
                diff.append(tup1)
                break
        finished.remove((sim,dura))
    FMT = '%H:%M:%S.%f'
    for a,b in diff:
        tdelta = datetime.strptime(b, FMT) - datetime.strptime(a, FMT)
        p=tdelta.seconds+(tdelta.microseconds)/(1000000)
        diff1.append(p)
    mean=sum(diff1)/len(diff1)
    med=statistics.median(diff1) 
    return mean,med

#To find mean and median of job completion time    
def job_analysis(lines):
    job_dict={}
    diff=[]
    diff1=[]
    for line in lines:
        if not line:
            continue
        a,b=line.strip().rsplit(',',1)
        _,q=a.split(' ')
        q=q.replace(',','.')
        key,_=b.split('_')
        if key in job_dict.keys():
            tup=(q,b)
            job_dict[key].append(tup)
        else:
            job_dict[key]=[]
            tup=(q,b)
            job_dict[key].append(tup)
    for key in job_dict:
        z=job_dict[key][0][0]
        x=job_dict[key][-1][0]
        tup1=(x,z)
        diff.append(tup1)
    FMT = '%H:%M:%S.%f'
    for a,b in diff:
        tdelta = datetime.strptime(a, FMT) - datetime.strptime(b, FMT)
        p=tdelta.seconds+(tdelta.microseconds)/(1000000)
        diff1.append(p)
    mean=sum(diff1)/len(diff1)
    med=statistics.median(diff1) 
    return mean,med
    
#Grouping the tasks into each worker for analysis
def master_analysis(lines):
    work1=[]
    work2=[]
    work3=[]
    for line in lines:
        if not line:
            continue
        a,b=line.strip().rsplit(',',1)
        c,dura=b.split(':')
        c=c.strip()
        dura=int(dura.strip())
        if c=="Worker_1":
            work1.append(dura)
        if c=="Worker_2":
            work2.append(dura)
        if c=="Worker_3":
            work3.append(dura)
    return work1,work2,work3
'''    
def plot_time_vs_task(y_axes,title1):
    x_axes=[i+1 for i in range(len(y_axes))]
    fig = go.Figure([go.Scatter(x=x_axes, y=y_axes)])
    fig.update_layout(
        autosize=False,
        width=1000,
        height=500,
        title=title1,
        template="simple_white",
    )
    fig.update_xaxes(title="Time")
    fig.update_yaxes(title="Tasks")
    return fig
'''  

#plotting the graph of no_of_tasks_scheduled_on_each_worker vs time
def plot_time_vs_task1(w11,w22,w33):
    x1=[i+1 for i in range(len(w11))]
    x2=[i+1 for i in range(len(w22))]
    x3=[i+1 for i in range(len(w33))]
    plt.plot(x1, w11, label = "Worker 1")
    plt.plot(x2, w22, label = "Worker 2")
    plt.plot(x3, w33, label = "Worker 3")
    plt.xlabel('Time') 
    plt.ylabel('No of Tasks Sheduled') 
    plt.title('Worker Analysis') 
    plt.legend() 
    plt.savefig('Task_sheduled_on_workers.png')
    
    
if __name__=="__main__":
    w1=sys.argv[1] #log file of worker 1
    w2=sys.argv[2] #log file of worker 2
    w3=sys.argv[3] #log file of worker 3
    m1=sys.argv[4] #log file of master
    m2=sys.argv[5] #log file of master for graph analysis
    worker1=open(w1,"r")
    worker2=open(w2,"r")
    worker3=open(w3,"r")
    master_log=open(m1,"r")
    master_ana=open(m2,"r")
    work1 = worker1.read().splitlines()
    work2 = worker2.read().splitlines()
    work3 = worker3.read().splitlines()
    master1 = master_log.read().splitlines()
    master2 = master_ana.read().splitlines()
    #print(len(work3))
    a=len(work1)
    b=len(work2)
    c=len(work3)
    if a>0:
        mean,med=worker_analysis(work1)
        print("Mean of tasks scheduled on worker1: ",mean)
        print("Median of tasks scheduled on worker1: ",med)
    if a==0:
        print("No Tasks scheduled on worker1")
    if b>0:
        mean1,med1=worker_analysis(work2)
        print("Mean of tasks scheduled on worker2: ",mean1)
        print("Median of tasks scheduled on worker2: ",med1)
    if b==0:
        print("No Tasks scheduled on worker2")
    if c>0:
        mean2,med2=worker_analysis(work3)
        print("Mean of tasks scheduled on worker3: ",mean2)
        print("Median of tasks scheduled on worker3: ",med2)
    if c==0:
        print("No Tasks scheduled on worker3")
    d=len(master1)
    if d>0:
        mean3,med3=job_analysis(master1)
        print("Mean of Jobs Recevied on Master: ",mean3)
        print("Median of Jobs Recevied on Master: ",med3)
    if d==0:
        print("No Jobs Recevied at Master")
    e=len(master2)
    if e>0:
        worker11,worker22,worker33=master_analysis(master2)
        worker11.insert(0,0)
        worker22.insert(0,0)
        worker33.insert(0,0)
        plot_time_vs_task1(worker11,worker22,worker33)
    worker1.close()
    worker2.close()
    worker3.close()
    master_log.close()
    master_ana.close()
    
