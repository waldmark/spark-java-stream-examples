package com.objectpartners.spark.rt911.common.components;

import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Map911Call implements Function<String, RealTime911> {
    private static Logger logger = LoggerFactory.getLogger(Map911Call.class);

    @Override
    public RealTime911 call(String line) throws Exception {
        RealTime911 call = new RealTime911();
        // split on, but not inside, quoted field
        String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (columns.length < 8) {
            String[] values = {"", "", "", "", "", "", ""}; // 7 values
            System.arraycopy(columns, 0, values, 0, columns.length);
            call.setAddress(values[0]);
            call.setCallType(values[1]);
            call.setDateTime(values[2]);
            call.setLatitude(values[3]);
            call.setLongitude(values[4]);
            call.setReportLocation(values[5].replaceAll("\"", ""));
            call.setIncidentId(values[6]);
        } else {
            logger.warn("bad row " + line);
        }
        return call;
    }
}
