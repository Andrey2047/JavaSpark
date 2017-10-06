import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by andriiko on 3/7/2017.
 */
public class WorldCountEample {

    public static void main(String[] args) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile("myFile.txt")
                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .foreach(stringIntegerTuple2 -> System.out.println(stringIntegerTuple2._1 + "  " + stringIntegerTuple2._2));

    }
}
