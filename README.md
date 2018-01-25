# Couchbase XDCR Sample Project

This project makes use of the Couchbase Multi-Cluster Aware Java client.  Using this client,
database loads will automatically transfer to alternative clusters if nodes become unavailable.  See
the Tutorial document in this repo for a walk through of setting up a multi-master, multi-region
cluster.

This client is currently in development, and available only as an Enterprise Edition feature
(as of 1/24/2018).  You will not be able to build this sample without the client.  For more
information, please contact a Couchbase sales representative.

Installation
* Java 8 or higher
* Maven

To run the example

    mvn compile
    mvn exec:java
or

    mvn compile
    mvn exec:exec@daemon

e.g. mvn exec:java -Dexec.args="-c 10.0.0.1,10.0.0.2,10.0.0.3:10.0.1.1,10.0.1.2,10.0.1.3 -b my_bucket -i jdoe -p password -r 1" runs a single thread reading random documents.
      
       
