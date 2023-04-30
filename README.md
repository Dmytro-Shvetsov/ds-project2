# Book shop using gRPC

### Instalation

To run the following app you need to
  * clone the repo
  * open 3 terminals in the project folder
  * run "python node.py [node_id]" for each of the terminals. You should use 0, 1, 2 ids.


### Usage:
  * Initialize the number of processes for each of the node by running "Local_store_ps [num_procs]"
  * Create chain by running "Create_chain". You are able to list the chain by running "List_chain".
  It is also possible to remove and restore heads by running "Remove_head" and "Restore_head"
  * Now you are able to write and read by using "Write [book_name] [price]" and "Read [book_name]"
  * You can list all of the books available by running "List_books"
  * You can list the status of each of the books by running "Data_status"
  * To terminate the app you should press ctrl+c in each of the terminals
  
  
  
### Project members:
  * Illia Tsiporenko
  * Dmytro Shvetsov 
  
