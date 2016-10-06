package com.objectpartners.spark.rt911.streaming.sockets;


import com.objectpartners.spark.rt911.common.components.SparkAnalysis;
import com.objectpartners.spark.rt911.common.components.SparkAnalysisWriter;
import com.objectpartners.spark.rt911.streaming.CallProcessing;
import com.objectpartners.spark.rt911.streaming.SparkRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {"com.objectpartners.spark.rt911.common", "com.objectpartners.spark.rt911.streaming.sockets"})
public class RT911Config {

    private long duration = 8000L;
    private int port = 9999;
    private String dataFileName = "Seattle_Real_Time_Fire_911_Calls_Chrono.csv.gz";
//    private String dataFileName = "Seattle_Real_Time_Fire_911_Calls_10_Test.csv.gz";
    private boolean sortByFrequency = false;
    private boolean filterFire = true;

    @Bean
    CallProcessing callProcessingSpark() {
        return new CallProcessingSpark(duration, sortByFrequency, filterFire);
    }

    @Bean
    CallProducingServer callProducingServer() {
        return new CallProducingServer(port, dataFileName);
    }

    @Bean
    SparkRunner sparkFullDemoRunner() {
        return new SparkFullDemoRunner();
    }

    @Bean
    SparkAnalysis sparkAnalysis() {
        return new SparkAnalysis();
    }

    @Bean
    SparkAnalysisWriter sparkAnalysisWriter() {
        return new SparkAnalysisWriter();
    }


}
