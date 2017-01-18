package json;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
public class json {
	
	

		private static final String NAME = "JavaWordCount";
		private static final Pattern SPACE = Pattern.compile(" ");
		//private static final paths ="/"
		String master = System.getProperty("spark.master");
		public static void main(String[] args) {
			
			// 1. Definir un SparkContext
			String master = System.getProperty("spark.master");
			//String master = System.getProperty("spark.master");
			//JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME,
			SparkConf sconf = new SparkConf().setAppName("prueba2.txt");
			sconf.setMaster("local[2]");
			JavaSparkContext ctx = new JavaSparkContext(sconf);
			//master == null ? "local" : master));
			SQLContext sql = SQLContext.getOrCreate(ctx.sc());
			// 2. Resolver nuestro problema
			Dataset<Row> dataset = sql.read().option("header", true).option("inferSchema", true).csv("prueba2.csv");
			Dataset<Row> manufacturas = dataset.select(dataset.col("valor1")).distinct();
			dataset.printSchema();
			manufacturas.show();
			// 3. Liberar recursos
			ctx.stop();
			ctx.close();
			}
	}


