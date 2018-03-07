package edu.uncc.cloud;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by sachin on 11/30/2015.
 */
public class Util {

    public static Tweet parseTweet(String tweetString) {
        try {
            JSONObject tweetJson = (JSONObject) new JSONParser().parse(tweetString);
            String tweetId = (String) tweetJson.get("id_str");
            String place = "";
            if (tweetJson.get("place") != null) {
                place = (String) ((JSONObject) tweetJson.get("place")).get("name");
            }
            String user = "";
            String lang = "";
            if (tweetJson.get("user") != null) {
                user = (String) ((JSONObject) tweetJson.get("user")).get("name");
                lang = (String) ((JSONObject) tweetJson.get("user")).get("lang");
            }
            String tweetText = (String) tweetJson.get("text");

            //clean tweet text
            tweetText = tweetText.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim();
//            String geo = "";
//            if (tweetJson.get("geo") != null) {
//                geo = tweetJson.get("geo").toString();
//            }
//
//            if (geo != null && geo.trim().length() > 0)
//                geoCnt++;
            if (lang.equals("en")) {
                Tweet tweet = new Tweet(tweetId, place, user, tweetText);
                tweet.setLang(lang);
                return tweet;
            }
        } catch (Exception e) {
        }

        return null;
    }

    public static boolean isTweetValid(String text) {
        String regex = "[A-Za-z0-9. ]";
        return Pattern.matches(regex, text);
    }

    private static StanfordCoreNLP pipeline;

    static {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }

    public static int getSentimentValue(String line) {

        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;

            List<Double> sentiments = new ArrayList<Double>();
            List<Integer> sizes = new ArrayList<Integer>();

            double sum = 0;
            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
                sentiments.add(new Double(sentiment));
                sum += sentiment;
                sizes.add(partText.length());
            }

            double averageSentiment = (sentiments.size() > 0) ? sum / sentiments.size() : 0.0;

            if (sentiments.size() == 0) {
                mainSentiment = -1;
            }

//            System.out.println("debug: main: " + mainSentiment);
//            System.out.println("debug: avg: " + averageSentiment);
        }
        return mainSentiment;
    }


    public static String getSentimentCategory(int sentiment) {
        if (sentiment < 2)
            return Sentiments.Negative.toString();
        else if (sentiment > 2)
            return Sentiments.Positive.toString();
        else
            return Sentiments.Neutral.toString();
    }

    public static int getSentimentValue1(String line) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                System.out.println("sentiment: " + sentiment);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
            return 0;
        } else return mainSentiment;

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
        //Map<Integer, CorefChain> graph =
        //       document.get(CorefCoreAnnotations.CorefChainAnnotation.class);

    }


    public static void main(String[] args) {
        String test = "\u0627\u0639\u0632\u0627\u0626\u064a \u0627\u0644\u0641\u0648\u0644\u0648\u0631\u0632 \u0627\u0644\u062c\u0648 \u0645\u0648 \u0645\u0627\u0644 \u062f\u0648\u0627\u0645 \n\n\n\n\n\u0627\u0644\u0627\u062a\u062c\u0627\u0647 \u0627\u0644\u0649 \u0645\u0634\u0631\u0641 \u0645\u0639\u0631\u0636 \u0627\u0644\u062a\u0644\u0641\u0648\u0646\u0627\u062a \n\n\u0642\u0628\u0648 \ud83d\ude99\ud83d\udca8\ud83d\udca8\ud83d\udca8\ud83d\udca8\ud83d\udca8\ud83d\udca8\ud83d\udca8";

        ///"From seas to mountains, valleys to forests. Israel has it all. http:\\/\\/t.co\\/a7uJCt4h Book a trip to Israel at http:\\/\\/t.co\\/clQRKK5k";
//        System.out.println(isTweetValid(test));
        String testVeryNegative = "very bad bad boy is me";//1
        String testNegative = "I am very bad";//2
        String testNeutral = "today is a holiday";//2
        String testPositive = "I am good";//2

        int Val = getSentimentValue(testPositive);
        System.out.println("Sentiment : " + Val);
    }
}


enum Sentiments {
    Negative, Neutral, Positive
}