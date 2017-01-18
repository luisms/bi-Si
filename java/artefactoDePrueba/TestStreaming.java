package artefactoDePrueba;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter.TwitterFacade;
import twitter4j.Status;
import twitter4j.auth.Authorization;

public class TestStreaming {
	private static final String PATH = "/home/luis/";

	public static void main(String[] args) throws Exception {
		// 1. Definir el objeto configurador de Spark
		String master = System.getProperty("spark.master");
		// JavaSparkContext ctx = new
		// JavaSparkContext(SparkConfigs.create("StreamingTwitter", master ==
		// null ? "local[2]" : master));
		SparkConf sconf = new SparkConf().setAppName("twitter");
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);

		ctx.setLogLevel("WARN");
		// 2. Twitter credentials from twitter.properties
		TwitterFacade.configureTwitterCredentials();
		Authorization twitter = TwitterFacade.getAuthority();
		String[] filters = TwitterFacade.getKeys();
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc, twitter, filters);
		JavaDStream<String> filtered = stream.map(status -> "" + status.getText());

		filtered.foreachRDD(rdd -> {
			LocalDateTime lc = LocalDateTime.now();
			//System.out.println(rdd.collect());//
			rdd.saveAsTextFile(PATH + Path.SEPARATOR + lc.toEpochSecond(ZoneOffset.UTC));
		});
		// 3. Abrir canal de datos
		ssc.start();
		ssc.awaitTermination();
	}

}
