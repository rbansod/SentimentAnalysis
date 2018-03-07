package edu.uncc.cloud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by sachin on 11/30/2015.
 */
public class Training {

    public static final String trainingDataSet = "D:\\uncc\\fall 15\\ITCS-6190-8190 Cloud Computing for Data Analysis\\project\\00.json";

    private static final FlatMapFunction<String, Tweet> TWEET_EXTRACTOR =
            new FlatMapFunction<String, Tweet>() {
                @Override
                public Iterable<Tweet> call(String s) throws Exception {
//                    System.out.println("s = " + s);
//                    ArrayList<Tweet> tweets = new ArrayList<Tweet>();
//                    tweets.add(Util.parseTweet(s));
//                    return Collections.list(Util.parseTweet(s));
                    return Arrays.asList(Util.parseTweet(s));
                }
            };

    private static final FlatMapFunction<Tweet, String> TWEET_TEXT_EXTRACTOR =
            new FlatMapFunction<Tweet, String>() {
                @Override
                public Iterable<String> call(Tweet s) throws Exception {

                    String text = "";
                    if (s != null && s.getText() != null && Util.isTweetValid(s.getText()))
                        text = s.getText();
                    return Arrays.asList(text.split("\\s"));
                }
            };


    HashMap<String, HashMap> categoryFeaturesMap = new HashMap<>();
    HashMap<String, Double> categoryFeaturesCountMap = new HashMap<>();

    HashMap<String, List<String>> classificationMap = new HashMap<>();
    HashMap<String, Double> categoryDocMap = new HashMap<>();

    private double totalFeaturesCount = 0;
    HashSet<String> globalFeatures = new HashSet<>();

    private double docSize = 0;
    HashMap<String, Double> probCategoryDocMap = new HashMap<>();
    HashMap<String, HashMap<String, Double>> probCategoryFeaturesMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        Training training = new Training();
//        training.testRun();
        training.actualTest();
//        training.loadDataFromHive(args);
    }

    private void loadDataFromHive(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("edu.uncc.cloud.Training").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

//        SQLContext sqlContext = new SQLContext(context.sc());
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(context.sc());
//        sqlContext.sql("use rbansod");

        DataFrame df = sqlContext.sql(" SELECT * FROM twitter_data_1gb limit 5");
//        DataFrame df = sqlContext.sql("FROM twitter_data_1gb SELECT idstr,text, username, place, coorinates, geo limit 5");
//        DataFrame df = sqlContext.sql("show tables");

        List<Row> rows = df.collectAsList();
        for (Row row : rows) {
            System.out.println(row.getString(0));
        }

        /*JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<Tweet> tweetsRDD = file.flatMap(TWEET_EXTRACTOR);

//        JavaRDD<String> tweetTextRDD = tweetsRDD.flatMap(TWEET_TEXT_EXTRACTOR);
        tweetsRDD.foreach(tweet -> Util.getSentimentValue(tweet.getText()));

        tweetsRDD.saveAsTextFile(args[1]);*/
    }


    private void actualTest() throws IOException {
        this.readTweets(trainingDataSet);
        System.out.println("categoryDocMap = " + categoryDocMap);
        System.out.println("categoryFeaturesMap = " + categoryFeaturesMap);
        this.calcTrainingProbabilites();
        String test1 = "it honestly feels like a shot (at least it did to me)";
        this.predict(test1);
    }

    private void testRun() throws IOException {
        ArrayList<Tweet> tweetList = this.loadTestTweets();
        for (Tweet tweet : tweetList) {
            this.trainAlgorithm(tweet);
            docSize++;
        }
        System.out.println("categoryDocMap = " + categoryDocMap);
        System.out.println("categoryFeaturesMap = " + categoryFeaturesMap);
        this.calcTrainingProbabilites();
        String test1 = "Mary had a lamb.";
        String test2 = "Mary had a white fleece.";
        this.predict(test1);
        this.predict(test2);
    }

    public void trainAlgorithm(Tweet tweet) {
//        for (Tweet tweet : tweetList) {
        //String[] words = instring.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
        List<String> features = Arrays.asList(tweet.getText().replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split(" "));

        String catg = tweet.getClas();
        if (categoryDocMap.containsKey(catg)) {
            double count = categoryDocMap.get(catg);
            categoryDocMap.put(catg, (count + 1));
        } else {
            categoryDocMap.put(catg, 1.0);
        }

        HashMap<String, Double> featuresMap;
        double featureCount = 0;
        if (categoryFeaturesMap.containsKey(catg)) {
            featuresMap = categoryFeaturesMap.get(catg);
            featureCount = categoryFeaturesCountMap.get(catg);
        } else {
            featuresMap = new HashMap<String, Double>();
        }
        featureCount += features.size();
        totalFeaturesCount += features.size();

        for (String feature : features) {
            if (feature.length() > 0) {
                globalFeatures.add(feature);

                if (featuresMap.containsKey(feature)) {
                    double count = featuresMap.get(feature);
                    featuresMap.put(feature, (count + 1));
                } else {
                    featuresMap.put(feature, 1.0);
                }
            }
        }
        categoryFeaturesMap.put(catg, featuresMap);
        categoryFeaturesCountMap.put(catg, featureCount);
//        }

    }

    private void calcTrainingProbabilites() {
        for (String catg : categoryDocMap.keySet()) {
            double prob = categoryDocMap.get(catg) / docSize;
            probCategoryDocMap.put(catg, prob);
        }

        System.out.println("probCategoryDocMap = " + probCategoryDocMap);


        for (String catg : categoryFeaturesMap.keySet()) {
            HashMap<String, Double> featuresMap = categoryFeaturesMap.get(catg);
            HashMap<String, Double> probFeaturesMap = categoryFeaturesMap.get(catg);

            double totalFeaturesCnt = 0;
            for (String feature : featuresMap.keySet()) {
                totalFeaturesCnt += featuresMap.get(feature);
            }

            for (String feature : globalFeatures) {
                double featureCountInClass = 0;
                if (featuresMap.containsKey(feature))
                    featureCountInClass = featuresMap.get(feature);
                double totalFeatureInClass = totalFeaturesCnt;
                double totalFeatures = globalFeatures.size();
                double prob = this.getProbabilityofFeatureXForGiveClass(featureCountInClass, totalFeatureInClass, totalFeatures);
                probFeaturesMap.put(feature, prob);
            }
            /*double totalFeaturesCnt = 0;
            for (String feature : featuresMap.keySet()) {
                totalFeaturesCnt += featuresMap.get(feature);
            }

            for (String feature : featuresMap.keySet()) {
                double prob = featuresMap.get(feature) / totalFeaturesCnt;
                probFeaturesMap.put(feature, prob);
            }*/
            probCategoryFeaturesMap.put(catg, probFeaturesMap);
        }

        System.out.println("probCategoryFeaturesMap = " + probCategoryFeaturesMap);

    }

    private String predict(String text) {
        List<String> features = Arrays.asList(text.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split(" "));
        HashMap<String, Double> probCategories = new HashMap<>();
        for (String catg : categoryFeaturesMap.keySet()) {
            double prob = probCategoryDocMap.get(catg);
            for (String feature : features) {
                Map map = probCategoryFeaturesMap.get(catg);

                //Note: decide what prob to be considered when the word is completely new i.e not present in the training word dictionary
                double temp = 1;
                if (map.get(feature) != null) {
                    temp = (double) map.get(feature);
                }
//                System.out.println("catg=" + catg + ";feature = " + feature + ";prob=" + temp);
                prob *= temp;
            }
            probCategories.put(catg, prob);
        }

        System.out.println("probCategories = " + probCategories);
        String predictedCategory = getMaxCategory(probCategories);
        System.out.println("predictedCategory = " + predictedCategory);
        return predictedCategory;
    }

    private String getMaxCategory(HashMap<String, Double> probCategories) {
        double max = 0;
        String category = "";
        for (String catg : probCategories.keySet()) {
            double prob = probCategories.get(catg);
            if (max < prob) {
                max = prob;
                category = catg;
            }
        }
        return category;
    }

    private double getProbabilityofFeatureXForGiveClass(double featureCountInClass, double totalFeatureInClass, double totalFeatures) {
        double alpha = 1;
        return (featureCountInClass + alpha) / (totalFeatures + (alpha * totalFeatureInClass));
    }


    private ArrayList<Tweet> loadTestTweets() throws IOException {
        String file = "D:\\uncc\\fall 15\\ITCS-6190-8190 Cloud Computing for Data Analysis\\project\\test.txt";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String temp;
        ArrayList<Tweet> tweetList = new ArrayList<>();
        int count = 0;
        while ((temp = br.readLine()) != null) {
            String tokens[] = temp.split("\t");
            Tweet tweet = new Tweet(tokens[0]);

            tweet.setClas(tokens[1]);
//             tweet = parseTweet(temp);
            System.out.println(tweet);
            tweetList.add(tweet);
        }
        br.close();

        return tweetList;
    }

    private void runSpark(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("edu.uncc.cloud.Training").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<Tweet> tweetsRDD = file.flatMap(TWEET_EXTRACTOR);

//        JavaRDD<String> tweetTextRDD = tweetsRDD.flatMap(TWEET_TEXT_EXTRACTOR);
//        tweetsRDD.foreach(tweet -> Util.getSentimentValue(tweet.getText()));

        tweetsRDD.saveAsTextFile(args[1]);
        conf.log().debug("end of spark context");
    }

    private void readTweets(String location) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(location)));
        String temp;
        ArrayList<String> inputList = new ArrayList<>();
        int count = 0;
        while ((temp = br.readLine()) != null) {
            Tweet tweet = null;
            tweet = Util.parseTweet(temp);
            System.out.println(tweet);
            if (tweet != null && tweet.getText() != null && tweet.getText().length() > 0) {
                int sentimentValue = Util.getSentimentValue(tweet.getText());
                String catg = Util.getSentimentCategory(sentimentValue);
                tweet.setClas(catg);
                this.trainAlgorithm(tweet);
                docSize++;
            }

//            if (tweet.getPlace() != null && tweet.getPlace().trim().length() > 0)
//                count++;
        }
//        System.out.println("place count = " + count);
//        System.out.println("geoCnt count = " + geoCnt);
        br.close();
    }


    private int geoCnt = 0;

    /*
    private Tweet parseTweet(String tweetString) {
        try {
            JSONObject tweetJson = (JSONObject) new JSONParser().parse(tweetString);
            String tweetId = (String) tweetJson.get("id_str");
            String place = "";
            if (tweetJson.get("place") != null) {
                place = (String) ((JSONObject) tweetJson.get("place")).get("name");
            }
            String user = "";
            if (tweetJson.get("user") != null) {
                user = (String) ((JSONObject) tweetJson.get("user")).get("name");
            }
            String tweetText = (String) tweetJson.get("text");
            String geo = "";
            if (tweetJson.get("geo") != null) {
                geo = tweetJson.get("geo").toString();
            }

            if (geo != null && geo.trim().length() > 0)
                geoCnt++;
            return new Tweet(tweetId, place, user, tweetText);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }
    */
}
