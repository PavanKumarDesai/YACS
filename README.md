
Introduction:

YACS(Yet Another Centralized Scheduler) is our final project. YACS is the centralized scheduling framework. It consists of a Master and several workers. The Master listens for Job requests from the client and makes decisions on scheduling tasks on each Worker, it also manages the resources. On the other hand, Worker executes the task received from master and reports back to Master when the task is executed. Here our framework consists of one Master and three Workers, where the Master schedules the jobs on each worker based on scheduling criteria such as RANDOM scheduling, ROUND ROBIN scheduling, LEAST LOADED scheduling. 

Related work:

YACS, is the simulation of YARN in Hadoop. So we went in studying the documentation of YARN in Hadoop 2.0 in order to understand the insights of the YARN architecture and we also tried to study various similar architectures. Additionally we also studied some client-server models.

Design:

As mentioned earlier, our framework consists of one Master process and three Worker process. Master listens for Job requests from the client and examines the status of each worker and assign each task to respective worker based on scheduling criteria. All the communication between master and worker happens through “message passing”, more specifically through socket programming.
Whereas the sole work of each Worker is to, listen from master for any tasks, as soon as it receives the task, add the task to its pool, execute it and report back to master when the task gets executed.
To utilize the workers efficiently, we create each thread for each job request, and inside a job we create each thread for each of the task. Creating these threads, gives the possibility of race condition, which we handle by utilizing various mutex locks and semaphores. As the master should also listen from workers, the updates of the task assigned to them, a thread is created and assigned for this purpose.

Detailed Steps of execution for each Job request sent from client are as follows:

Step 1: Master listening for job request from client.\
Step 2: On receiving the job from client, creates a thread for the job then it parses the job and adds the map tasks and reduce tasks to be scheduled to map_list and reduce_list respectively.\
Step 3: After adding the tasks to their respective list, master first schedules the map tasks on each available workers based on scheduling criteria provided. At the tine of scheduling, if there are no workers free, it waits for a second and then again tries to schedule.\
Step 4: After assigning map tasks to each worker, it waits for them to be finished, meanwhile the workers execute the given task and report back to master.\
Step 5: After completing each map task(on the update made by workers), master removes it from map list.\
Step 6: Once master ensures that there are no more map tasks of the job remaining, it then schedules the reduce tasks of the job in the same way it schedules map tasks.\
Step 7: Once all reduce tasks are completed(on the update made by workers), this marks the end of job. Both Master and Workers log the required information for further analysis.\

Data Structures used: 
Mostly Dictionaries and Lists as they are easy and effective to use. Dictionaries are used to maintain information about each worker and update them as and when necessary.

Once all the job requests are completed from client, we try to analyse the task completion time, job completion time and status of each workers at different time intervals from the logged information.


Inferences:

By analysing the results one can come upto following inferences(here we measure performance in terms of even distribution of load on each worker):

1.	Random scheduling mostly gives average performance in terms of distributing the load to each worker. And gives better performance than other two, when the number of tasks in map or reduce job is not evenly distributed and when there is much difference in the number of slots on each worker.
2.	Least Loaded scheduling works best when there is no much difference between number of slots in each worker. 
3.	Round robin scheduling works best when number of tasks in each map or reduce job is a multiple of number of workers.


Problems Encountered:

Various problems were encountered while doing the project such as managing threads, maintaining the appropriate connections and updating them accordingly. Most of the problems were solved by inspecting the functionality of each functions and rephrasing them accordingly. We also had to use facility of internet to solve some problems that arose during modelling the architecture.


Conclusion:
As someone rightly said, “Education without application is just entertainment.” We understood the complexity in building and deploying such frameworks in reality. We also had the opportunity of implementing the theories we learnt in the class. On and all we had very much fun doing this project and learnt a lot of things form this. 


The Given Project consists of three files, master.py, worker.py and analysis.py

Steps for execution:

Part-1:
1.First Run the worker.py on three different terminals with three different ports in correspondence to config file.

2.Then Run master.py on another terminal by providing the config file and scheduling alogorithm as command line arguments.

3.When both master and workers are up and running, then run the requests.py file on another terminal to generate requests and send them to master.

Part-2:
1.Once all the requests are scheduled and finished by master, close both master and workers.

2.From the logs generated, to perform analysis, run analysis.py. This takes necessary file path to perform analyis,
  in total it requires 5 command line aruguemnts, they are:
  (i) "Worker_1.log"
  (ii)"Worker_2.log"
  (iii)"Worker_3.log"
  (iv)"log_from_master_(scheduling_criteria).log"
  (v)"master_analysis_(scheduling_criteria).log"
  
3.After doing this the terminal will display the necessary details and also with generate and save an image in your local file system, which represents the graph of
  number_of_tasks_scheduled_on_each_worker vs time.
