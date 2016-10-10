package com.objectpartners.spark.rt911.streaming.kafka;

import com.objectpartners.spark.rt911.common.components.Map911Call;
import com.objectpartners.spark.rt911.common.components.SparkAnalysis;
import com.objectpartners.spark.rt911.common.components.SparkAnalysisWriter;
import com.objectpartners.spark.rt911.common.domain.RealTime911;
import com.objectpartners.spark.rt911.streaming.CallProcessing;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

@Component
class CallProcessingKafkaSpark implements Serializable, CallProcessing {
    static final long serialVersionUID = 100L;

    @Autowired
    Map911Call map911Call;

    @Autowired
    SparkAnalysis sparkAnalysis;

    @Autowired
    SparkAnalysisWriter sparkAnalysisWriter;


    private long streamBatchInterval;
    private boolean isSortedByFrquency;
    private boolean isFilteredOnFire;
    private String topic;
    private String broker;


    CallProcessingKafkaSpark(long streamBatchInterval, boolean sortByFrquency, boolean filterFire, String topic, String broker) {
        this.streamBatchInterval = streamBatchInterval;
        this.isSortedByFrquency = sortByFrquency;
        this.isFilteredOnFire = filterFire;
        this.topic = topic;
        this.broker = broker;
    }

    public JavaStreamingContext processStream() throws SparkException {

        // create a local Spark Context with two working threads
        SparkConf streamingConf = new SparkConf().setMaster("local[2]").setAppName("911Calls");
        streamingConf.set("spark.streaming.stopGracefullyOnShutdown", "true");

        // create a Spark Java streaming context, with stream batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(streamingConf, Durations.milliseconds(streamBatchInterval));

        // set checkpoint for demo
        jssc.checkpoint(System.getProperty("java.io.tmpdir"));

        jssc.sparkContext().setLogLevel(Level.OFF.toString()); // turn off Spark logging

        // set up receive data stream from kafka
        final Set<String> topicsSet = new HashSet<>(Arrays.asList(topic));
        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", broker);

        // create a Discretized Stream of Java String objects
        // as a direct stream from kafka (zookeeper is not an intermediate)
        JavaPairInputDStream<String, String> rawDataLines =
                KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );

        // get the kafka topic data
        JavaDStream<String> lines = rawDataLines.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

        // transform to RealTime911 objects
        JavaDStream<RealTime911> callData = lines.map(map911Call);

        callData = callData.map(x -> { // clean data
            String callType = x.getCallType().replaceAll("\"", "").replaceAll("[-|,]", "");
            x.setCallType(callType);
            return x;
        }).filter(pair -> { // filter data
            return !isFilteredOnFire || pair.getCallType().matches("(?i).*\\bFire\\b.*");
        });

        // create pairs with key of call type and value of a Java call object
        JavaPairDStream<String, ArrayList<RealTime911>> pairs = callData.mapToPair(rt911 ->
                new Tuple2<>(rt911.getCallType(), new ArrayList<>(Arrays.asList(rt911))));

        // micro batch summary - a micro batch grouped by call type
        // create reduced pairs with key of call type and value of the list of call java call objects of that type
        JavaPairDStream<String, ArrayList<RealTime911>> reducedPairs = pairs.reduceByKey((a, b) -> {
            a.addAll(b);
            return a;
        });

        // state summary
        // update a stateful count of calls by call type
        JavaPairDStream<String, Long> reducedState = pairs.mapToPair(pair ->
                new Tuple2<>(pair._1(), 1L))
                .updateStateByKey(
                        (List<Long> calls, Optional<Long> currentCount) -> {
                            Long sum = currentCount.or(0L) + ((long) calls.size());
                            return Optional.of(sum);
                        });


        // produce output

        sparkAnalysisWriter.reportCurrentCallTypeMicroBatch(reducedPairs);
        sparkAnalysisWriter.reportCurrentCallTypeState(reducedState, isSortedByFrquency);

        return jssc;
    }

    public long getStreamBatchInterval() {
        return streamBatchInterval;
    }

}
