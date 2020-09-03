# CS134_Distributed_Systems

## Introduction
This repository contains my CS134 Distributed Systems Projects taken in Spring 2020.

### Languge Covered: Golang

## Components
[viewservice](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/viewservice) is the source code for the master server, view service, which monitors the status of each available servers. This implements [assignment 2](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment2.html) part A for this course.

[pbservice](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/pbservice) is the source code for the primary/backup key/value service, [assignment 2](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment2.html) part B for this course.

[paxos](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/paxos) is the source code for a library that implements paxos protocol, which is used in all the components below. This is part A for [assignment 3](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment3.html).

[kvpaxos](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/kvpaxos) is the source code for fault-tolerant, Paxos-based Key/Value Server. This is part B for [assignment 3](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment3.html). 


[shardmaster](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/shardmaster) is the source code for a shard master server which in charge of shard configuration and load-balancing. It uses Paxos library to be fault-tolerant. This is Part A for [assignment4](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment4.html).

[shardkv](https://github.com/Luke-ZL/Distributed_Systems_Projects/tree/master/src/shardkv) is the source code for Sharded Key/Value Server, an implementation of a fault-tolerant, sharded database. This is Part B for [assignment4](http://web.cs.ucla.edu/~ravi/CS134_S20/assignment4.html).
