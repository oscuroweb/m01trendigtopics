package org.rhcalero.bigdata.module1.spark.twitter.model;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * Tweet:
 * <p>
 * POJO class that represent a tweet
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 16, 2016
 */
public class Tweet implements Serializable {

    /** Generated serial version */
    private static final long serialVersionUID = -5199342210390071678L;

    /** Log instance. */
    private static Logger log = Logger.getLogger(Tweet.class.getName());
    /** Separator character. */
    private static final String SEPARATOR = "\\t";
    /** Minimum number of fields. */
    private static final int NUM_FIELDS = 4;

    /** Identifier. */
    Long id;
    /** User name. */
    String username;
    /** Message of the tweet. */
    String tweet;
    /** Date of the tweet. */
    String date;

    public Tweet(String line) {
        String[] lineArray = line.split(SEPARATOR);

        if (lineArray.length >= NUM_FIELDS) {
            id = Long.parseLong(lineArray[0]);
            username = lineArray[1];
            tweet = lineArray[2];
            date = lineArray[3];
        } else {
            String errorMessage = "[ERROR] Incorrect passwd line number of fields for line " + line;
            log.fatal(errorMessage);
            throw new RuntimeException(errorMessage);
        }

    }

    /**
     * Method getId.
     * <p>
     * Get method for attribute id
     * </p>
     * 
     * @return the id
     */
    public Long getId() {
        return id;
    }

    /**
     * Method setId.
     * <p>
     * Set method for attribute id
     * </p>
     * 
     * @param id the id to set
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Method getUsername.
     * <p>
     * Get method for attribute username
     * </p>
     * 
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Method setUsername.
     * <p>
     * Set method for attribute username
     * </p>
     * 
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Method getTweet.
     * <p>
     * Get method for attribute tweet
     * </p>
     * 
     * @return the tweet
     */
    public String getTweet() {
        return tweet;
    }

    /**
     * Method setTweet.
     * <p>
     * Set method for attribute tweet
     * </p>
     * 
     * @param tweet the tweet to set
     */
    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    /**
     * Method getDate.
     * <p>
     * Get method for attribute date
     * </p>
     * 
     * @return the date
     */
    public String getDate() {
        return date;
    }

    /**
     * Method setDate.
     * <p>
     * Set method for attribute date
     * </p>
     * 
     * @param date the date to set
     */
    public void setDate(String date) {
        this.date = date;
    }

    /**
     * Method toString.
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(username);
        builder.append("\t ");
        builder.append(tweet.length());
        builder.append("\t ");
        builder.append(date);
        return builder.toString();
    }

}
