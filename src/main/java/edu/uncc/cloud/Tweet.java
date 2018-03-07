package edu.uncc.cloud;

import java.util.Enumeration;

/**
 * Created by sachin on 11/30/2015.
 */
public class Tweet {
    private String tweetId;
    private String place;
    private String user;
    private String text;
    private String lang;
    private String clas;


    public Tweet(String tweetId, String place, String user, String text) {
        this.tweetId = tweetId;
        this.place = place;
        this.user = user;
        this.text = text;
    }

    public Tweet(String text) {
        this.text = text;
    }

    public String getTweetId() {
        return tweetId;
    }

    public String getPlace() {
        return place;
    }

    public String getUser() {
        return user;
    }

    public String getText() {
        return text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }


    public String getClas() {
        return clas;
    }

    public void setClas(String clas) {
        this.clas = clas;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "tweetId='" + tweetId + '\'' +
                ", place='" + place + '\'' +
                ", user='" + user + '\'' +
                ", text='" + text + '\'' +
                ", lang='" + lang + '\'' +
                ", clas='" + clas + '\'' +
                '}';
    }
}
