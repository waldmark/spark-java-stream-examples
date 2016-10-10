## Spark Java Streaming Example
![Alt text](images/spark-logo-trademark.png)


<br/>
</br>
<p>
Java examples of Apache Spark 2.0.0 Stream processing running against a simulated feed of 911 calls. 
</p>

<i>Requirements:</i>
* Java 8 installed
* Kafka installed (version 0.10.0.1)-- needed to to run the Kafka example

This code provides examples of using Spark to consume 911 calls in the form of csv data in the following ways:

1. From a gzipped file, where each line represents a separate 911 call.
2. From a socket server which reads and serves the gzipped call data a line at a time.
3. From an Apacke Kafka (version 0.10.0.0) topic, which requires a producer that reads and feeds the 911 data to the topic.

The examples can be run from an IDE (like IntelliJ), or from a runnable jar. See instructions below on building the runnable <i>uber-jar</i>.

### Stand alone processing from a file
The class com.objectpartners.spark.rt911.standalone.MainApplication has a runnable main. It processes all data in the gzip file and provides a summary to standard out.

### Streaming data and Spark
Streaming has two examples:

1. Stream processing from a socket server
2. Stream processing from a Kafka topic

These can be run from the class com.objectpartners.spark.rt911.streaming.StreamingMainApplication

### Socket version

To run the socket version, run StreamingMainApplication with no program arguments. This example is self contained and will
start and run a socket server which will send clients 911 call data. The socket client runs within the Spark application code,
and automatically pulls data from the socket server on the Spark Streaming micro batch interval.

### Kafka version

To run the Kafka version, run with the com.objectpartners.spark.rt911.streaming.StreamingMainApplication with one (case insensitive) argument:'kafka'. 
The Kafka version has some pre-conditions:

1. A Kafka server must be up and running, which in turn requires that
2. A Zookeeper server is up and running

Instructions on running these can be found on the Apache Kafka web site, at <a href="http://kafka.apache.org/documentation.html#quickstart" target="_blank">starting kafka instructions</a>.

Once the Kafka server is up and running, a client must feed it a stream of 911 calls from one of the project's test data files (located in the src/main/resources directory).

A runnable jar can be built from a separate [project](https://github.com/waldmark/kafka-demo) that starts a Kafka producer client and reads from a file supplied as runtime argument. 

## Building a runnable jar
A standalone jar can be created using Gradle. In the project root directory, in a terminal run gradle:

1. gradle clean build
2. gradle shadowjar

The uber-jar will be built and placed in the {$project.dir}/build/libs directory.

## Running from the jar
To run the socket streaming example from the jar:
<pre><code>java -jar spark-java-streaming-example-0.1-all.jar</code></pre>

To run the Kafka streaming example from the jar:

1. You must install Kafka (the demo has been developed with Kafka 0.10.0.1)
2. In a new terminal, start zookeeper on its default port
3. In a new terminal, start Kafka on its default port
4. In a new terminal, start the Kafka 911 producer client
    <pre><code>java -jar kafka-demo-0.1-all.jar kafka</code></pre>
5. In a new terminal, run the jar
    <pre><code>java -jar spark-java-streaming-example-0.1-all.jar kafka</code></pre>
    
Note the argument provided (e.g. 'kafka'), this runs the Kafka version of the demo.


## Resources
In src/main/resources are two gzips containing 911 call data in csv format:

1. Seattle_Real_Time_Fire_911_Calls_10_Test.csv.gz contains 10 911 calls (10 lines) and can be used for simple testing.
    Note that the application assumes the first line contains header data, so only 9 calls are actually processed.
2. Seattle_Real_Time_Fire_911_Calls_Chrono.csv.gz 
    A chronologically ordered set of (lots of) calls.
    










 

