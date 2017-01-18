package big;

import java.util.regex.Pattern;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class bigData {
	private static final String NAME = "cosa";
	// private static final String URL =
	// "jdbc:mysql://localhost:3306/peliculas";
	private static final String TABLE = "data";
	private static final String PATH = "/home/luis/Escritorio/TOS_DI-20161026_1219-V6.3.0/workspace/out1.csv";
	String master = System.getProperty("spark.master");

	public static void main(String[] args) throws AnalysisException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf sconf = new SparkConf().setAppName("prueba2.txt");
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);

		String master = System.getProperty("spark.master");
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		Dataset<Row> dataset = sql.read().option("inferSchema", true).option("inferSchema", true).json("games");
		
		JavaRDD<RatioCB> aux =dataset.select(dataset.col("id").as("user1"), dataset.col("idFilm").as("film1"))
				.join(dataset.select(dataset.col("idUser").as("user2"), dataset.col("idFilm").as("film2")))
				.where("user1 = user2").where("film1 <> film2").select("film1", "film2").javaRDD()
				.mapToPair(x -> new Tuple2<Row, Integer>(x, 1)).reduceByKey((x, y) -> x + y).map(x -> new RatioCB(x));
		sql.createDataFrame(aux, RatioCB.class).write().jdbc(URL, TABLE + "CB", properties);
		ctx.stop();
		ctx.close();
	}
}
