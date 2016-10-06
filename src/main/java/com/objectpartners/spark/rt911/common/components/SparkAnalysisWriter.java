package com.objectpartners.spark.rt911.common.components;


import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class SparkAnalysisWriter implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger LOG = LoggerFactory.getLogger(SparkAnalysisWriter.class);


    public void reportCurrentCallTypeMicroBatch(JavaPairDStream<String, ArrayList<RealTime911>> reduced) {
        reduced.foreachRDD( rdd -> {
            if(rdd.count() < 1) { // dont show empty micro batch (e.g. no data received in the last time interval)
                return;
            }
            Date now = new Date(System.currentTimeMillis());
            LOG.info("\n\n-------------------------------------------<<< MICRO BATCH " + now + " >>>" +
                    "------------------------------------------");

            JavaPairRDD<String, ArrayList<RealTime911>> sorted = rdd.sortByKey();
            sorted.foreach( record -> {
                buildLogRecord(record._1(), record._2());
            });
        });
    }

    private void buildLogRecord(final String callTypeKey, final List<RealTime911> calls) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\n%-10s calls: %d", callTypeKey, calls.size()));
        for(RealTime911 rt: calls) {
            String itemData = String.format("\n   %-36s %-24s %-42s (%.4f, %.4f)",
                    rt.getCallType(),
                    rt.getDateTime(),
                    rt.getAddress(),
                    !rt.getLongitude().isEmpty() ? Float.valueOf(rt.getLongitude().trim()) : 0,
                    !rt.getLatitude().isEmpty() ? Float.valueOf(rt.getLatitude().trim()) : 0);
            sb.append(itemData);
        }
        LOG.info(sb.toString()+"\n");
    }

    public void reportCurrentCallTypeState(JavaPairDStream<String, Long>  reducedState, boolean isSortedByFrequency) {
        // display the call type state (running count of call types)
        reducedState.foreachRDD( rrdd -> {
            LOG.info("---------------------------------------------------------------\n");
            LOG.info("          State Total: \n");
            LOG.info("---------------------------------------------------------------\n");

            List<Tuple2<String, Long>> sorted;
            if (isSortedByFrequency) {
                JavaPairRDD<Long, String> stateCounts = rrdd.mapToPair(Tuple2::swap); // 3. swap so we can order by frequency
                JavaPairRDD<Long, String> orderedStateCounts = stateCounts.sortByKey(false); // 4. sort by key (frequency)
                JavaPairRDD<String, Long> counts = orderedStateCounts.mapToPair(Tuple2::swap); // 5. swap back so the key is again callType
                sorted = counts.collect();
            } else {
                JavaPairRDD<String, Long> counts = rrdd.sortByKey();
                sorted = counts.collect();
            }

            sorted.forEach( record -> {
                String type = String.format("  %-40s  total received: %d", record._1(), record._2());
                LOG.info(type+ "\n");
            });


        });
    }

}
