package org.rhcalero.bigdata.module1.spark.twitter;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.twitter.model.Tweet;

import scala.Tuple2;

/**
 * MostActiveUser:
 * <p>
 * Get the most active users from a list of tweets.
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 18, 2016
 */
public class MostActiveUser {

    /** Log instance. */
    private static Logger log = Logger.getLogger(MostActiveUser.class.getName());

    /**
     * 
     * Method main.
     * <p>
     * Execute Most Active User program.
     * </p>
     * 
     * @param args Input parameters
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

        // STEP 5: Get Java RDD pair where key is user and value is 1
        JavaPairRDD<String, Integer> usersPair = filterdLines.mapToPair(line -> getUserTuple(line));
        log.debug("[DEBUG] STEP 5: Get Java RDD pair where key is user and value is 1");

        // STEP 6: Reduce pairs
        JavaPairRDD<String, Integer> usersCountPair = usersPair.reduceByKey((value1, value2) -> value1 + value2);

        // STEP 7: Transform reduced pair (where key is username and value is count) to a new one where key is count and
        // value is username
        JavaPairRDD<Integer, String> countUsersPair = usersCountPair
                .mapToPair(userCountTuple -> new Tuple2<Integer, String>(userCountTuple._2(), userCountTuple._1()));
        log.debug("[DEBUG] STEP 7: Transform reduced pair (where key is username and value is count) to a new one where key is count and value is username");

        // STEP 8 Sort users by number of tweet.
        JavaPairRDD<Integer, String> sortedCountUsersPair = countUsersPair.sortByKey(false);
        log.debug("[DEBUG] STEP 8 Sort users by number of tweet.");

        // STEP 9: Get the most active users
        Tuple2<Integer, String> activeUser = sortedCountUsersPair.first();

        // Get computing time
        long computingTime = System.currentTimeMillis() - initTime;

        // STEP 10: Print the result
        StringBuffer tupleStr = new StringBuffer();
        tupleStr.append(activeUser._2()).append(": ").append(activeUser._1());
        System.out.println(tupleStr.toString());

        System.out.println("Computing time: " + computingTime);

        // STEP 11: Stop the spark context
        context.stop();
        context.close();

    }

    /**
     * Method getUserTuple.
     * <p>
     * DESC.
     * </p>
     * 
     * @param line
     * @return
     */
    private static Tuple2<String, Integer> getUserTuple(String line) {
        Tweet tweet = new Tweet(line);
        return new Tuple2<String, Integer>(tweet.getUsername(), 1);
    }
}
