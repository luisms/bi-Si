package artefactoDePrueba;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class TestWordCount {

	// sparkContext
	private static final String NAME = "JavaWordCount";
	private static final Pattern SPACE = Pattern.compile(" ");

	String master = System.getProperty("spark.master");
	// JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME,
	// master==null? "local":master));

	public static void main(String[] args) throws Exception {

		SparkConf sconf = new SparkConf().setAppName("TestWordCount");
		sconf.setMaster("local[2]");

		JavaSparkContext ctx = new JavaSparkContext(sconf);

		JavaRDD<String> lines = ctx.textFile("/home/luis/Escritorio/TOS_DI-20161026_1219-V6.3.0/workspace/out.csv");
		JavaRDD<String> words = lines.flatMap(x -> ((List<String>) (Arrays.asList(SPACE.split(x)))).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> (i1 + i2));
		System.out.println(counts.collectAsMap());
		ctx.stop();
		ctx.close();
	}
}
