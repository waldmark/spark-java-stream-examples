package com.objectpartners.spark.rt911.streaming.sockets;

import com.objectpartners.spark.rt911.streaming.CallProcessing;
import com.objectpartners.spark.rt911.streaming.SparkRunner;
import org.apache.spark.SparkException;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class SparkFullDemoRunner implements SparkRunner {
    private static Logger logger = LoggerFactory.getLogger(SparkFullDemoRunner.class);

    @Autowired
    @Qualifier("callProcessingSpark")
    private CallProcessing sparkProcessor;

    @Autowired
    private CallProducingServer producer;

    public void runSparkStreamProcessing() {
        final JavaStreamingContext jssc;

        try {
            jssc = sparkProcessor.processStream();
        } catch (SparkException se) {
            logger.error(se.getMessage());
            return;
        }

        jssc.start();

        final Thread runSimulation = new Thread(() -> {
            logger.info("starting call producing client...");
            producer.run();
            logger.info("911 call production completed...");
            try {
                // wait for twice the batch interval to allow spark to finish
                Thread.sleep((sparkProcessor.getStreamBatchInterval() * 2));
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
            logger.info("calling stop on Spark Streaming Context ....");
            jssc.stop();
        });
        runSimulation.start();

        try {
            jssc.awaitTermination();
            logger.info("Spark termintation ....");
        } catch (InterruptedException ie) {
            logger.error(ie.getMessage());
        } finally {
            jssc.close();
        }

        logger.info("application completed ....");
    }
}
