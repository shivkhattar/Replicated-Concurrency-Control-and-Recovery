# Replicated-Concurrency-Control-and-Recovery

A distributed database, complete with multiversion concurrency control, deadlock detection, replication, and failure recovery.

### Requirements
- `java 8`
- `maven`

### Building the application

`mvn clean install`

### Running the application
- Without arguments  
  runs all all the input files in the folder `src/main/resources/input/input-1` by default  
  `mvn exec:java -Dexec.mainClass=com.nyu.repcrec.RepCRec`
- With arguments  
  application takes a single argument can be a `filepath` or a `folder_path` containing all the input files   
  `mvn exec:java -Dexec.mainClass=com.nyu.repcrec.RepCRec -Dexec.args=src/main/resources/input/input-1`
 
 ### Running the application using Vagrant
 - install vagrant
 - run `vagrant up`
 - ssh into the vagrant box using `vagrant ssh`
 - cd into the source dir `cd /vagrant`
 - execute the commands for running the application
 
 ### Running the application using Reprozip
 Reprozip file `RepCRec.rpz` is present in the directory.
 #### Reprounzip Installation
 - Upgrade setuptools  
  `pip install -U setuptools`  
 - Install reprounzip with all available plugins  
  `pip install -U reprounzip[all]`  
 
 #### Unpack and run using reprounzip
 - `reprounzip vagrant setup RepCRec.rpz <dest_directory>`  
 - `reprounzip vagrant run <dest_directory>`
