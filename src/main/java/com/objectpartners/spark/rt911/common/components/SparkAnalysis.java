package com.objectpartners.spark.rt911.common.components;

import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class SparkAnalysis implements Serializable {
    static final long serialVersionUID = 100L;

    // clean the raw input data
    public JavaDStream<RealTime911> cleanData(JavaDStream<RealTime911> callData) {
        return callData.map(x -> {
            String callType = x.getCallType().replaceAll("\"", "").replaceAll("[-|,]", "");
            x.setCallType(callType);
            return x;
        });
    }

    // filter calls that contain 'fire'
    public JavaDStream<RealTime911> filterFireCalls(JavaDStream<RealTime911> callData) {
        return callData.filter(x -> x.getCallType().matches("(?i).*\\bFire\\b.*"));
    }

    // create DStream of Java pairs with callType as first field and list of RealTime911 objects as the second
    public JavaPairDStream<String, ArrayList<RealTime911>> mapCallsByCallType(JavaDStream<RealTime911> callData) {
        return callData.mapToPair(rt911 ->
                new Tuple2<>(rt911.getCallType(), new ArrayList<>(Arrays.asList(rt911))));
    }

    // reduce call pair DStream by key (e.g. by callType)
    public JavaPairDStream<String, ArrayList<RealTime911>> reduceCallMapByKey(JavaPairDStream<String, ArrayList<RealTime911>> callPairs) {
        return callPairs.reduceByKey((a, b) -> {
            a.addAll(b);
            return a;
        });
    }

    // mutate the state by key to compute a running total of 911 calls by call type
    public JavaPairDStream<String, Long> reduceStateByCallType(JavaPairDStream<String, ArrayList<RealTime911>> callPairs) {
        return callPairs.mapToPair(pair -> new Tuple2<>(pair._1(), 1L)).updateStateByKey(pairFunction);
    }

    // create a pair function for tracking the state of the call type count
    private Function2<List<Long>, Optional<Long>, Optional<Long>> pairFunction =
            (List<Long> calls, Optional<Long> currentCount) -> {
                Long sum = currentCount.or(0L) + ((long) calls.size());
                return Optional.of(sum);
            };

}
