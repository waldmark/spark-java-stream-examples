package com.objectpartners.spark.rt911.streaming.kafka;

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
class SparkKafkaRunner implements SparkRunner {
    private static Logger logger = LoggerFactory.getLogger(SparkKafkaRunner.class);

    @Autowired
    @Qualifier("callProcessingKafkaSpark")
    private CallProcessing sparkProcessor;


    public void runSparkStreamProcessing() {
        try (JavaStreamingContext jssc = sparkProcessor.processStream()) {
            jssc.start();
            jssc.awaitTermination();
            logger.info("Spark stream processing termintation ....");
        } catch (InterruptedException | SparkException e) {
            logger.error(e.getMessage());
        }

        logger.info("SparkKafkaRunner completed ....");
    }
}
