package com.objectpartners.spark.rt911.standalone;

import com.objectpartners.spark.rt911.common.domain.RealTime911;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.ListIterator;

class SparkProcessor implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger logger = LoggerFactory.getLogger(SparkProcessor.class);
    private String dataFileName = "Seattle_Real_Time_Fire_911_Calls_Chrono.csv.gz";


    void processFileData() {

        File inputFile = getDataFile(dataFileName);
        if(null == inputFile) {
            logger.error("no input file found, stopping execution...");
            System.exit(-1);
        }

        // set execution configuration
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc); // SQLContext entry point for working with structured data

        JavaRDD<String> dataWithHeader = sc.textFile(inputFile.getAbsolutePath());

        logger.info("found data file " + dataWithHeader.toString() + " at: " + inputFile.getAbsolutePath());
        dataWithHeader.cache();

        // get the header data
        final String firstLine = dataWithHeader.first();
        logger.info("first line: " + firstLine);

        // filter out header data
        JavaRDD<String> data = dataWithHeader.filter((Function<String, Boolean>) s -> !s.equals(firstLine));
        long dataCount = data.count();
        logger.info("data count: " + dataCount);

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<RealTime911> callData = data.map(
                (Function<String, RealTime911>) line -> {
                    RealTime911 call = new RealTime911();
                    // split on , but not inside qupoted field
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
                        logger.error("bad row " + line);
                    }
                    return call;
                });

        // process with Spark SQL
        Dataset<Row> ds = sqlContext.createDataFrame(callData, RealTime911.class); // Apply a schema to an RDD of JavaBeans and register it as a table.

        String [] cols = ds.columns();
        for(String col: cols) {
            System.out.println("Column: " + col);
        }

        ds.createOrReplaceTempView("callData"); // SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> alarmsDF = sqlContext.sql("SELECT callType FROM callData");


        // *************************************************************************************************************
        // sort by frequecy on uncleansed data
        // *************************************************************************************************************
        RelationalGroupedDataset gd = alarmsDF.groupBy("callType"); // 1. group by callType
        Dataset<Row> gddf = gd.count(); // 2. count by group
        gddf = gddf.orderBy("count"); // 3. order by count
        List<Row> list = gddf.collectAsList(); // 4. get a List

        logger.info("\n==================================SORTED BY FREQUENCY=========================================\n");
        logger.info("DataFrame: uncleansed count: " + list.size());

        ListIterator<Row> listIterator = list.listIterator(list.size());
        while(listIterator.hasPrevious()){
            Row r = listIterator.previous();
            logger.info("(" + r.get(0) + ", " + r.get(1) + ")");
        }

        // *************************************************************************************************************
        // sort by frequecy on cleansed data
        // *************************************************************************************************************

        JavaRDD<String> cleansedCallTypes = callData.map(x -> (
                x.getCallType().replaceAll("\"", "").replaceAll("[-|,]", "")));
        // create pair for reduction
        JavaPairRDD<String, Integer> cpairs = cleansedCallTypes.mapToPair(s -> new Tuple2<>(s, 1)); // 1. create pairs
        JavaPairRDD<String, Integer> creduced = cpairs.reduceByKey((a, b) -> a + b); // 2. reduce by callType
        // swap pair order so we can sort on frequency
        JavaPairRDD<Integer, String> scounts = creduced.mapToPair(Tuple2::swap); // 3. swap so we can order by frequency
        JavaPairRDD<Integer, String> oscounts = scounts.sortByKey(); // 4. sort by key (frequency)
        // swap again so the key is call type, not frequency
        JavaPairRDD<String, Integer> xscounts = oscounts.mapToPair(Tuple2::swap); // 5. swap back so the key is again callType
        // get a list
        List<Tuple2<String, Integer>> cleansedList = xscounts.collect(); //6. get a List

        logger.info("\n==================================CLEANSED AND SORTED BY FREQUENCY=============================\n");
        logger.info("cleansed count: " + cleansedList.size());

        ListIterator<Tuple2<String, Integer>> cleansedListIterator = cleansedList.listIterator(cleansedList.size());
        while(cleansedListIterator.hasPrevious()){
            Tuple2<String, Integer> r = cleansedListIterator.previous();
            logger.info("(" + r._1 + ", " + r._2 + ")");
        } // should combine some entries that were duplicates
    }

    /*
     * try to get data file from within IDE, or from location we are running from,
     * if not found at IDE location then this should find the input data file when running as a deployed jar,
     * provided the file is located side by side with the runnable jar
     */
    private File getDataFile(String fileName) {
        File inputFile = null;
        try {
            URL data = SparkProcessor.class.getResource("/"+fileName);
            inputFile = new File(data.toURI());
            logger.info("trying with file  = " + inputFile.getAbsolutePath());

            if(!inputFile.exists()) {
                logger.info("trying over with file  = " + inputFile.getAbsolutePath());
                throw new RuntimeException("input file not found: " + inputFile.getAbsolutePath());
            }
        } catch (Exception e) {
            // no data found
            logger.error(e.getMessage());
        }
        return inputFile;
    }

    void setDataFileName(String dataFileName) {
        this.dataFileName = dataFileName;
    }
}
