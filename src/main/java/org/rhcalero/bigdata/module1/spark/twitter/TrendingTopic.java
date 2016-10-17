package org.rhcalero.bigdata.module1.spark.twitter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.twitter.model.Tweet;

import scala.Tuple2;

/**
 * 
 * TrendingTopic:
 * <p>
 * Java spark program to obtain a trending topic from a list of tweets stored in a file
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 16, 2016
 */
public class TrendingTopic {

    /** Log instance. */
    private static Logger log = Logger.getLogger(TrendingTopic.class.getName());

    /** White space constant. */
    private static final String WHITE_SPACE = " ";

    /** List of word to be skipped. */
    private static Pattern stopWords = Pattern
            .compile(
                    "^a$|^about$|^above$|^above$|^across$|^after$|^afterwards$|^again$|^against$|^all$|^almost$|^alone$|^along$|^already$|^also$|^although$|^always$|^am$|^among$|^amongst$|^amoungst$|^amount$|^ an$|^and$|^another$|^any$|^anyhow$|^anyone$|^anything$|^anyway$|^anywhere$|^are$|^around$|^as$|^at$|^back$|^be$|^became$|^because$|^become$|^becomes$|^becoming$|^been$|^before$|^beforehand$|^behind$|^being$|^below$|^beside$|^besides$|^between$|^beyond$|^bill$|^both$|^bottom$|^but$|^by$|^call$|^can$|^cannot$|^cant$|^co$|^con$|^could$|^couldnt$|^cry$|^de$|^describe$|^detail$|^do$|^done$|^down$|^due$|^during$|^each$|^eg$|^eight$|^either$|^eleven$|^else$|^elsewhere$|^empty$|^enough$|^etc$|^even$|^ever$|^every$|^everyone$|^everything$|^everywhere$|^except$|^few$|^fifteen$|^fify$|^fill$|^find$|^fire$|^first$|^five$|^for$|^former$|^formerly$|^forty$|^found$|^four$|^from$|^front$|^full$|^further$|^get$|^give$|^go$|^had$|^has$|^hasnt$|^have$|^he$|^hence$|^her$|^here$|^hereafter$|^hereby$|^herein$|^hereupon$|^hers$|^herself$|^him$|^himself$|^his$|^how$|^however$|^hundred$|^ie$|^if$|^in$|^inc$|^indeed$|^interest$|^into$|^is$|^it$|^its$|^itself$|^keep$|^last$|^latter$|^latterly$|^least$|^less$|^ltd$|^made$|^many$|^may$|^me$|^meanwhile$|^might$|^mill$|^mine$|^more$|^moreover$|^most$|^mostly$|^move$|^much$|^must$|^my$|^myself$|^name$|^namely$|^neither$|^never$|^nevertheless$|^next$|^nine$|^no$|^nobody$|^none$|^noone$|^nor$|^not$|^nothing$|^now$|^nowhere$|^of$|^off$|^often$|^on$|^once$|^one$|^only$|^onto$|^or$|^other$|^others$|^otherwise$|^our$|^ours$|^ourselves$|^out$|^over$|^own$|^part$|^per$|^perhaps$|^please$|^put$|^rather$|^re$|^same$|^see$|^seem$|^seemed$|^seeming$|^seems$|^serious$|^several$|^she$|^should$|^show$|^side$|^since$|^sincere$|^six$|^sixty$|^so$|^some$|^somehow$|^someone$|^something$|^sometime$|^sometimes$|^somewhere$|^still$|^such$|^system$|^take$|^ten$|^than$|^that$|^the$|^their$|^them$|^themselves$|^then$|^thence$|^there$|^thereafter$|^thereby$|^therefore$|^therein$|^thereupon$|^these$|^they$|^thickv$|^thin$|^third$|^this$|^those$|^though$|^three$|^through$|^throughout$|^thru$|^thus$|^to$|^together$|^too$|^top$|^toward$|^towards$|^twelve$|^twenty$|^two$|^un$|^under$|^until$|^up$|^upon$|^us$|^very$|^via$|^was$|^we$|^well$|^were$|^what$|^whatever$|^when$|^whence$|^whenever$|^where$|^whereafter$|^whereas$|^whereby$|^wherein$|^whereupon$|^wherever$|^whether$|^which$|^while$|^whither$|^who$|^whoever$|^whole$|^whom$|^whose$|^why$|^will$|^with$|^within$|^without$|^would$|^yet$|^you$|^your$|^yours$|^yourself$|^yourselves$|^the$|^rt$|^http:\\/\\/.*$",
                    Pattern.CASE_INSENSITIVE);

    /** List of punctuation marks. */
    private static List<String> specialCharacters = Arrays
            .asList(new String[] { ".", ",", ":", "-", StringUtils.EMPTY });

    /** Number of TTs to take. */
    private static int NUMBER_OF_TTS = 10;

    /**
     * 
     * Method main.
     * <p>
     * Execute Trending Topic program.
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

        // STEP 5: Get words from Java RDD lines
        JavaRDD<String> words = filterdLines.flatMap(line -> getTweetWords(line));
        log.debug("[DEBUG] STEP 5: Get words from Java RDD lines");

        // STEP 6: Remove punctuation marks of words and parse to lower case. */
        JavaRDD<String> cleanWords = words.map(word -> cleanWord(word));
        log.debug("STEP 6: Remove punctuation marks of words and parse to lower case");

        // STEP 7: Filter by no skipped word.*/
        JavaRDD<String> validWords = cleanWords.filter(word -> isValidWord(word));
        log.debug("STEP 7: Filter by no skipped word");

        // STEP 8: Remove special words
        JavaRDD<String> finalWords = validWords.filter(word -> !specialCharacters.contains(word));

        // STEP 9: Get a Java Pair RDD where key is a word and value is 1
        JavaPairRDD<String, Integer> wordsPair = finalWords.mapToPair(word -> new Tuple2<String, Integer>(word.trim(),
                1));
        log.debug("STEP 9: Get a Java Pair RDD where key is a word and value is 1");

        // STEP 10: Reduced word pair with the sum of the values
        JavaPairRDD<String, Integer> reducedWordPair = wordsPair.reduceByKey((value1, value2) -> value1 + value2);
        log.debug("STEP 10: Reduced word pair with the sum of the values");

        // STEP 11: Transform reduced pair (where key is word and value is count) to a new one where key is count and
        // value is word
        JavaPairRDD<Integer, String> wordCountReducedPair = reducedWordPair
                .mapToPair(wordCountTuple -> new Tuple2<Integer, String>(wordCountTuple._2(), wordCountTuple._1()));
        log.debug("STEP 11: Transform reduced pair (where key is word and value is count) to a new one where key is count and value is word");

        // STEP 12: Sort by key reduced word pair in descent mode
        JavaPairRDD<Integer, String> sortedReducedWordPair = wordCountReducedPair.sortByKey(false);
        log.debug("STEP 12: Sort by key reduced word pair");

        // STEP 13: Take the first 10 TT
        List<Tuple2<Integer, String>> wordsCountList = sortedReducedWordPair.take(NUMBER_OF_TTS);
        log.debug("STEP 13: Take the first 10 TT");

        // Get computing time
        long computingTime = System.currentTimeMillis() - initTime;

        // STEP 14: Print the result
        for (Tuple2<Integer, String> tuple : wordsCountList) {
            StringBuffer tupleStr = new StringBuffer();
            tupleStr.append(tuple._2()).append(": ").append(tuple._1());
            System.out.println(tupleStr.toString());
        }
        System.out.println("Computing time: " + computingTime);
        System.out.println("Total words: " + wordsCountList.size());

        // STEP 15: Stop the spark context
        context.stop();
        context.close();
    }

    /**
     * 
     * Method getTweetWords.
     * <p>
     * Obtain list of words from a tweet log line
     * </p>
     * 
     * @param line Tweet log line
     * @return List of tweets words
     * @throws Exception Generic exception
     */
    private static Iterator<String> getTweetWords(String line) throws Exception {
        Tweet tweet = new Tweet(line);
        Iterator<String> wordsIt = Arrays.asList(tweet.getTweet().split(WHITE_SPACE)).iterator();
        return wordsIt;
    }

    /**
     * 
     * Method cleanWord.
     * <p>
     * Remove punctuation marks to the word and parse it to lower case.
     * </p>
     * 
     * @param word Word to be cleaned
     * @return Cleaned word
     * @throws Exception Generic exception
     */
    private static String cleanWord(String word) throws Exception {
        String cleanWord = word;
        for (String mark : specialCharacters) {
            if (cleanWord.endsWith(mark)) {
                cleanWord = word.replace(mark, StringUtils.EMPTY);
            }
        }
        cleanWord = cleanWord.toLowerCase();

        return cleanWord;
    }

    /**
     * 
     * Method isValidWord.
     * <p>
     * Obtain if the word is a valid word.
     * </p>
     * 
     * @param word Word
     * @return True if word is a valid word. False in other case
     */
    private static boolean isValidWord(String word) {
        return !stopWords.matcher(word).find();
    }
}
