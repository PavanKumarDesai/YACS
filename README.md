# UE18CS322_BIG_DATA_YACS_FINALPROJECT
Final Project of UE18CS322 Course - YACS

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
