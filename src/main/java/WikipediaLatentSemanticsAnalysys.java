import com.google.common.collect.Lists;
import common.XmlInputFormat;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author andriiko
 * @since 10/4/2017
 */
public class WikipediaLatentSemanticsAnalysys {

    public static void main(String[] args) throws IOException {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        Configuration hadoopConf = new Configuration();
        hadoopConf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        hadoopConf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        long startTime = System.currentTimeMillis();

        JavaPairRDD<LongWritable, Text> articlesRDD =
                context.newAPIHadoopFile("src/main/resources/wikipedia.xml", XmlInputFormat.class, LongWritable.class, Text.class, hadoopConf);

        JavaRDD<String> rawXmls = articlesRDD.map(v1 -> v1._2().toString());
        JavaRDD<Optional<Tuple2>> plainTextRows = rawXmls.map(WikipediaLatentSemanticsAnalysys::wikiXmlToPlainText).sample(false, 0.005, 2);

        List<String> stopwordsList = Files.readAllLines(Paths.get("src/main/resources/stopwords.txt"));

        Broadcast<List<String>> stopwords = context.broadcast(stopwordsList);

        System.out.println(plainTextRows.count());

        JavaRDD<List<String>> lemmasRDD = plainTextRows.mapPartitions(tuple -> {
            StanfordCoreNLP nlpPipeline = createNLPPipeline();
            ArrayList<Optional<Tuple2>> texts = Lists.newArrayList(tuple);
            return texts.stream().filter(Optional::isPresent).map(t -> t.get()._2()).map(t -> plainTextToLemmas((String) t,
                    new HashSet<>(stopwords.getValue()), nlpPipeline)).collect(Collectors.toList()).iterator();
        });

        JavaRDD<Map<String, Long>> docTermsFreqs =
                lemmasRDD.map(v1 -> v1.stream().collect(Collectors.groupingBy(o -> o, Collectors.counting())));

        List<Long> docIds = docTermsFreqs.zipWithUniqueId().map(Tuple2::_2).collect();

        Integer numDocs = docIds.size();

        int numberOfTerms = 50000;

        Map<String, Integer> docFreqs =
                docTermsFreqs.flatMap(stringLongMap -> stringLongMap.keySet().iterator())
                        .mapToPair(s -> new Tuple2<>(s, 1))
                        .reduceByKey((v1, v2) -> v1 + v2)
                        .filter(v1 -> v1._2() > 1)
                        .top(numberOfTerms, new TupleComparator())
                        .stream()
                        .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        Map<String, Double> idfFreqs = calculateInverseDocumentFreqs(docFreqs, numDocs);

        Map<String, Double> bIdfs = context.broadcast(idfFreqs).getValue();

        JavaRDD<Map<String, Double>> termDocumentMatrix = docTermsFreqs.map(documentTermsFreqs -> {
            Long docTotalTerms = documentTermsFreqs.values().stream().reduce(0L, (aLong, aLong2) -> aLong + aLong2);
            return documentTermsFreqs.keySet().stream().collect(Collectors.toMap(o -> o, o -> bIdfs.get(o) * documentTermsFreqs.get(o) / docTotalTerms));
        });

    }

    private static Map<String, Double> calculateInverseDocumentFreqs(Map<String, Integer> docFreqs, Integer numDocs) {
        return docFreqs.keySet().stream().collect(Collectors.toMap(o -> o, o -> Math.log(numDocs / docFreqs.get(o))));
    }

    private static StanfordCoreNLP createNLPPipeline() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        return new StanfordCoreNLP(props);
    }

    private static boolean isOnlyLetters(String str) {
        return str.chars().mapToObj(Character::isLetter).reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2);
    }

    private static List<String> plainTextToLemmas(String text, Set<String> stopWords, StanfordCoreNLP pipeline) {
        Annotation doc = new Annotation(text);
        pipeline.annotate(doc);
        List<String> lemmas = new ArrayList<String>();
        List<CoreMap> sentenses = doc.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentenses) {
            List<CoreLabel> coreLabels = sentence.get(CoreAnnotations.TokensAnnotation.class);
            for (CoreLabel coreLabel : coreLabels) {
                String lemma = coreLabel.get(CoreAnnotations.LemmaAnnotation.class);
                if (lemma.length() > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
                    lemmas.add(lemma.toLowerCase());
                }
            }
        }

        return lemmas;
    }


    private static Optional<Tuple2> wikiXmlToPlainText(String xml) {
        EnglishWikipediaPage page = new EnglishWikipediaPage();
        WikipediaPage.readPage(page, xml);
        if (page.isEmpty()) return Optional.empty();
        else return Optional.of(new Tuple2<>(page.getTitle(), page.getContent()));
    }

    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o1._2 - o2._2();
        }
    }
}
