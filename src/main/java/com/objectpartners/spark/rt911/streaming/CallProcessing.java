package com.objectpartners.spark.rt911.streaming;


import org.apache.spark.SparkException;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public interface CallProcessing {
    JavaStreamingContext processStream() throws SparkException;
    long getStreamBatchInterval();
}
