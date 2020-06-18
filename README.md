# COVID-19 data analysis with MapReduce 

Simple data processing on COVID-19 dataset for Hadoop and MapReduce warm up, using local single node Hadoop setup (using HDP sandbox).

## Basic data analysis implementations:

    1. Count the total number of reported cases for every country/location till April 8th, 2020
    2. Report total number of deaths for every location/country in between a given range of dates
    3. Count the total number of cases per 1 million population for every country

## Instructions for running:

### Run a Hadoop environment with a VM Based Distribution

- Virtual Machine Installation: Download and install a virtual machine client. Here, I used VirtualBox (https://www.virtualbox.org/wiki/Downloads )

- Download a Virtual machine image: HDP Sandbox (version 3.0.1 at the time of writing). Download a distribution matching with your virtual machine client (recommended: Virtualbox image i.e .ova file).

- Start your VM client and load the Virtual machine image. Example: On VirtualBox select File -> Import Appliance. 
    - Click the folder icon and select the.ovf file from previous step. 
    - Recommended: Use a higher number of CPUs than default (1), such as 4, 6, 8+ (depending on how many CPUs are in your machine).
    - Wait for VirtualBox to load the image. (It may take some time, so sit back and relax)
    - Start the virtual machine by double-clicking its name in the menu.

- ssh into the virtual machine
    - For UNIX (Mac/Linux):
    
            Launch Terminal
            Type the following command
            ssh root@localhost -p 2222
            On first successfull login, you'll be asked to renew the password for root

- Test Hadoop Distributed File System: Execute "hdfs dfs -ls /" command on Terminal. You should see multiple directories listed by HDFS.

### Lists of commands used:

- To view content a file in HDFS use:

        hdfs dfs -cat path_in_hdfs
        
- Download the content of a sample text file

        wget http://<file_link>

- List the content of the current directory

        ls -lt

- List the content of some directories on HDFS
        hdfs dfs -ls /user

         hdfs dfs -ls /user/root

- Put the text file into HDFS

        hdfs dfs -put <data_file>.txt /user/root/<data_file>.txt

- Open a text file and copy the content of the program source into the text editor. Save it as <filename>.java
- Create a temporary build directory to store the compiled
         
         mkdir build
         
- Transfer <filename>.java to the sandbox
    - for UNIX (MacOS/Linux)
            
            scp -P host_path_wordcount_file root@localhost:/
            
- Compiling the code into Java bytecode

        javac -cp `hadoop classpath` <filename>.java -d build -Xlint

- Package the code into a JAR archive file
  
        jar -cvf <filename>.jar -C build/ .

         ls -lt

_(You should see there is a <filename>.jar file just being created)_

- Execute the MapReduce job with 2 parameters - input and output path

         hadoop jar <filename>.jar <filename> /user/root/<data_file>.txt /user/root/<output>

- View the content/result
  
        hdfs dfs -cat /user/root/<output>/*

- Remove the output directory
  
        hdfs dfs -rm -r /user/cloudera/<output>


