import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by andriiko on 3/10/2017.
 */
public class MusicRatings {

    public static final String ARTISTS_USERS_PATH = "C:\\edu\\workspace\\profiledata_06-May-2005\\user_artist_data.txt";
    public static final String ARTIST_DATE = "C:\\edu\\workspace\\profiledata_06-May-2005\\artist_data.txt";
    public static final String ARTIST_ALIAS = "C:\\edu\\workspace\\profiledata_06-May-2005\\artist_alias.txt";


    public static void main(String[] args) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rawUserArtistData = context.textFile(ARTISTS_USERS_PATH);
        JavaRDD<Double> map = rawUserArtistData.map(v1 -> new Double(v1.split(" ")[0]));

        JavaRDD<String> rawArtistData = context.textFile(ARTIST_DATE);
        JavaRDD<Tuple2<Integer, String>> artistByID = rawArtistData.map(line -> {
            String[] split = line.split("\\t");
            if(split[0].isEmpty()) {
                return new Tuple2<>(0, "");
            }
            try {
                return new Tuple2<>(Integer.valueOf(split[0]), split[1].trim());
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException ex){
                return new Tuple2<>(0, "");
            }
        });

        JavaRDD<String> rawArtistAlias = context.textFile(ARTIST_ALIAS);

        JavaRDD<Tuple2<Integer, Integer>> artistAlias = rawArtistAlias.map(line -> {
            String[] split = line.split("\\t");
            if(split[0].isEmpty()){
                return new Tuple2<>(0, 0);
            }
            return new Tuple2<>(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
        });

        Integer artId = artistAlias.filter(v1 -> v1._1.equals(6803336)).first()._2;
        System.out.println(artId);
        System.out.println(artistByID.filter(v1 -> v1._1.equals(artId)).first());


    }


}
