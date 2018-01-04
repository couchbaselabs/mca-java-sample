# Couchbase xDCR Sample Project

This project makes use of the 

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
      
       
