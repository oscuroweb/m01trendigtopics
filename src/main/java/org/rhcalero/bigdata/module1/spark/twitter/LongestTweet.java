package org.rhcalero.bigdata.module1.spark.twitter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * LongestTweet:
 * <p>
 * Get the size of most longer tweet to each users from a list of tweets
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 18, 2016
 */
public class LongestTweet {

    /** Log instance. */
    private static Logger log = Logger.getLogger(LongestTweet.class.getName());

    /**
     * 
     * Method main.
     * <p>
     * Execute longest tweet program.
     * </p>
     * 
     * @param args Input arguments
     */
    public static void main(String[] args) {

        // Validate arguments list. File name or directory is required
        if (args.length < 1) {
            log.fatal("[ERROR] RuntimeException: There must be at least one argument (a file name or directory)");
            throw new RuntimeException();
        }

        // Get initial time
        long initTime = System.currentTimeMillis();

        // STEP 1: Create a SparkConf object
        SparkConf conf = new SparkConf();

        // STEP 2: Create a Java Spark Context
        JavaSparkContext context = new JavaSparkContext(conf);

        // STEP 3: Get lines from file(s) and put it in a Java RDD instance
        JavaRDD<String> lines = context.textFile(args[0]);
        log.debug("[DEBUG] STEP 3: Get lines from file(s) and put it in a Java RDD instance");

        // STEP 4: Remove empty lines
        JavaRDD<String> filterdLines = lines.filter(line -> !line.isEmpty());
        log.debug("[DEBUG] STEP 4: Remove empty lines");

        // TODO Get longest tweet from each user

        // Get computing time
        long computingTime = System.currentTimeMillis() - initTime;

        // STEP 10: Print the result
        // TODO Print result
        System.out.println("Computing time: " + computingTime);

        // STEP 11: Stop the spark context
        context.stop();
        context.close();
    }

}
