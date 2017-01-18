package prueba;

import java.util.regex.Pattern;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class prueba2 {

	private static final String NAME = "JavaWordCount";
	private static final Pattern SPACE = Pattern.compile(" ");
	//private static final paths ="/"
	String master = System.getProperty("spark.master");
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		// 1. Definir un SparkContext
		
		//String master = System.getProperty("spark.master");
		//JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME,
		SparkConf sconf = new SparkConf().setAppName("prueba2.txt");
		sconf.setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(sconf);
		//master == null ? "local" : master));
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());
		// 2. Resolver nuestro problema
		Dataset<Row> dataset = sql.read().option("inferSchema", true).option("inferSchema", true).json("games");
		dataset.printSchema();
		Dataset<Row> datos1 = dataset.select(dataset.col("player")).distinct();
		
		Dataset<Row> datos = sql.read().option("inferSchema", true).option("inferSchema", true).json("user");
		datos.printSchema();
		Dataset<Row>datos2 = dataset.select(dataset.col("_id")).distinct();
		
		
		Dataset<Row> union = datos1.join(datos2);
		union.show();
		
		// 3. Liberar recursos
		ctx.stop();
		ctx.close();
		}
}
