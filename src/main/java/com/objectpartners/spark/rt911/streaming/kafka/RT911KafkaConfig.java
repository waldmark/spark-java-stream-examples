package com.objectpartners.spark.rt911.streaming.kafka;


import com.objectpartners.spark.rt911.streaming.CallProcessing;
import com.objectpartners.spark.rt911.streaming.SparkRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.objectpartners.spark.rt911.common", "com.objectpartners.spark.rt911.streaming.kafka"})
public class RT911KafkaConfig {

    private long windowDuration = 8000L;
    private boolean sortByFrquency = false;
    private String topic = "demo-topic";
    private String broker = "localhost:9092";
    private boolean filterFire = false;


    @Bean
    CallProcessing callProcessingKafkaSpark() {
        return new CallProcessingKafkaSpark(windowDuration, sortByFrquency, filterFire, topic, broker);
    }

    @Bean
    SparkRunner sparkKafkaRunner() {
        return new SparkKafkaRunner();
    }

}
