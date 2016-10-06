package com.objectpartners.spark.rt911.streaming.sockets;


import com.objectpartners.spark.rt911.common.components.Map911Call;
import com.objectpartners.spark.rt911.common.components.SparkAnalysis;
import com.objectpartners.spark.rt911.common.components.SparkAnalysisWriter;
import com.objectpartners.spark.rt911.common.domain.RealTime911;
import com.objectpartners.spark.rt911.streaming.CallProcessing;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;

@Component
class CallProcessingSpark implements Serializable, CallProcessing {
    static final long serialVersionUID = 100L;

    @Autowired
    Map911Call map911Call;

    @Autowired
    SparkAnalysis sparkAnalysis;

    @Autowired
    SparkAnalysisWriter sparkAnalysisWriter;

    private final long streamBatchInterval;
    private final boolean isSortedByFrquency;
    private final boolean isFilteredOnFire;

    CallProcessingSpark(long streamBatchInterval, boolean sortByFrquency, boolean filterFire) {
        this.streamBatchInterval = streamBatchInterval;
        this.isFilteredOnFire = filterFire;
        this.isSortedByFrquency = sortByFrquency;
    }

    public JavaStreamingContext processStream() throws SparkException {
        // create a local Spark Context with two working threads
        SparkConf streamingConf = new SparkConf().setMaster("local[2]").setAppName("911Calls");
        streamingConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

        // create a Spark Java streaming context, with stream batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(streamingConf, Durations.milliseconds(streamBatchInterval));
        jssc.checkpoint(System.getProperty("java.io.tmpdir")); // set checkpoint for demo
        jssc.sparkContext().setLogLevel(Level.OFF.toString()); // turn off Spark logging

        // receive data stream
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<RealTime911> callData = lines.map(map911Call); // transform to RealTime911 objects

        // analyze with Spark
        callData = sparkAnalysis.cleanData(callData);
        if(isFilteredOnFire) {
            sparkAnalysis.filterFireCalls(callData);
        }
        JavaPairDStream<String, ArrayList<RealTime911>> pairs = sparkAnalysis.mapCallsByCallType(callData);
        JavaPairDStream<String, ArrayList<RealTime911>>  reduced = sparkAnalysis.reduceCallMapByKey(pairs);
        JavaPairDStream<String, Long>  reducedState = sparkAnalysis.reduceStateByCallType(pairs);

        sparkAnalysisWriter.reportCurrentCallTypeMicroBatch(reduced);
        sparkAnalysisWriter.reportCurrentCallTypeState(reducedState, isSortedByFrquency);

        return jssc;
    }

    public long getStreamBatchInterval() {
        return streamBatchInterval;
    }
}
