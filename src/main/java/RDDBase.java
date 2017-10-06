import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by andriiko on 3/7/2017.
 */
public class RDDBase {

    public static void main(String[] args) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        context.addFile("myFile.txt");

        JavaRDD<String> lines = context.textFile(SparkFiles.get("myFile.txt"));
        System.out.println(lines.first());

        JavaRDD<String[]> words = lines.map(v1 -> v1.split(","));
        System.out.println("Words count = " + words.take(1).get(0).length);

        JavaRDD<String> stringJavaRDD = lines.flatMap(v1 -> Arrays.asList(v1.split(",")).iterator());

        System.out.println(stringJavaRDD.take(2));
    }
}
