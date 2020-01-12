Distributed Computing Project 3
================================

Submitted by - 

 Susan George 
 Mansi Mehta

to execute -
javac Coordinator.java
java Cordinator PP3-coordinator-conf.txt
javac Participant.java
java Participant PP3-participant-conf.txt

First Coordinator.java needs to be executed passing the location of PP3-coordinator-conf.txt as command line argument.
Once coordinator is running, we execute several instances of Participant.java with location of PP3-participant-conf.txt mentioned as command line argument

PP3-coordinator-conf.txt
==========================
The first parameter in the coordinator config file is port number
second is time for keeping messages stored.

PP3-participant-conf.txt
=========================
For the participant, the conf file contains input the below format:

10
     ---- unique id of participant
10log.txt  --- log file name
localhost 7000
  ---- ip and port number of coordinator separated by a single white space



We have considered test cases for all the commands. 

For our program,we have made the following assumptions:
	- A participant must register first before sending a multicast message.
	- We register each participant on different ports
	- Currently,we are limiting max number of threads to 8.This value can be adjusted in MAX_THREADS variable in Coordinator.java

*****************************************************************************************************************************************************
This project was done in its entirety by Susan George and Mansi Mehta. We hereby state that we have not received unauthorized help of any form.
*****************************************************************************************************************************************************

