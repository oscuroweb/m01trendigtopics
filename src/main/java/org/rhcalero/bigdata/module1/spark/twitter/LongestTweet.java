package org.rhcalero.bigdata.module1.spark.twitter;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.twitter.model.Tweet;

import scala.Tuple2;

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

        // STEP 5: Get tweet pair.
        JavaPairRDD<String, Tweet> userTweetPair = filterdLines.mapToPair(line -> getTweetTuple(line));
        log.debug("[DEBUG] STEP 5: Get tweet pair");

        // STEP 6: Get longest tweet for each user. */
        JavaPairRDD<String, Tweet> userLongestTweetPair = userTweetPair
                .reduceByKey((tweet1, tweet2) -> getLongestTweet(tweet1, tweet2));
        log.debug("[DEBUG] STEP 6: Get longest tweet for each user");

        // STEP 7: Order by user name
        JavaPairRDD<String, Tweet> sortedUserLongestTweetPair = userLongestTweetPair.sortByKey();
        log.debug("[DEBUG] STEP 7: Order by user name");

        // STEP 8: Collect the result
        List<Tuple2<String, Tweet>> usersLongestList = sortedUserLongestTweetPair.collect();
        log.debug("[DEBUG] STEP 8: Collect result");

        // Get computing time
        long computingTime = System.currentTimeMillis() - initTime;

        // STEP 9: Print the result
        for (Tuple2<String, Tweet> tweet : usersLongestList) {
            System.out.println(tweet._2().toString());
        }
        System.out.println("Computing time: " + computingTime);

        // STEP 10: Stop the spark context
        context.stop();
        context.close();
    }

    /**
     * 
     * Method getTweetTuple.
     * <p>
     * Obtain a tuple where key is the user name and value is the tweet.
     * </p>
     * 
     * @param line Line of tweet
     * @return Tuple username, tweet
     */
    private static Tuple2<String, Tweet> getTweetTuple(String line) {
        Tweet tweet = new Tweet(line);
        return new Tuple2<String, Tweet>(tweet.getUsername(), tweet);
    }

    /**
     * Method getLongestTweet.
     * <p>
     * Obtain the tweet which message is longest.
     * </p>
     * 
     * @param tweet1 First tweet
     * @param tweet2 Second tweet
     * @return Longest tweet
     */
    private static Tweet getLongestTweet(Tweet tweet1, Tweet tweet2) {
        Tweet longestTweet = null;
        if (tweet1.getTweet().length() >= tweet2.getTweet().length()) {
            longestTweet = tweet1;
        } else {
            longestTweet = tweet2;
        }
        return longestTweet;
    }

}
