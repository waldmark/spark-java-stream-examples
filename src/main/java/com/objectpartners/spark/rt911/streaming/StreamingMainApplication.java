package com.objectpartners.spark.rt911.streaming;

import com.objectpartners.spark.rt911.streaming.kafka.RT911KafkaConfig;
import com.objectpartners.spark.rt911.streaming.sockets.RT911Config;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class StreamingMainApplication {

    public static void main(final String[] args) throws InterruptedException {
        ApplicationContext ctx;
        SparkRunner runner = null;

        if(args.length == 0) {
            ctx = new AnnotationConfigApplicationContext(RT911Config.class);
            runner = (SparkRunner) ctx.getBean("sparkFullDemoRunner");
        } else if(args[0].equalsIgnoreCase("KAFKA")) {
            ctx = new AnnotationConfigApplicationContext(RT911KafkaConfig.class);
            runner = (SparkRunner) ctx.getBean("sparkKafkaRunner");
        } else {
            System.err.println("Argument [" + args[0] + "] must be 'KAFKA' to run with kafka streaming or unspecified to run standalone sreaming demo");
            System.exit(1);
        }

        if(null != runner) {
            runner.runSparkStreamProcessing();
        }
    }

}
