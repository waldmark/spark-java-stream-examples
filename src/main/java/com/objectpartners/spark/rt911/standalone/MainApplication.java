package com.objectpartners.spark.rt911.standalone;

public class MainApplication {

    public static void main(String[] args) {
        String dataFileName = "Seattle_Real_Time_Fire_911_Calls_Chrono.csv.gz";
        SparkProcessor sparkProcessor = new SparkProcessor();
        sparkProcessor.setDataFileName(dataFileName);
        sparkProcessor.processFileData();
    }
}

