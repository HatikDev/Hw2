package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.CountryCounter.countCountries;

public class SparkTest {
    final String testString1 = "8, 14:20, Russia, Russia";
    final String testString2 = "9, 18:15, Russia, Germany";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneCountry() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1));
        JavaRDD<Row> result = countCountries(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals(" 14");
        assert rowList.iterator().next().getString(1).equals(" Russia");
        assert rowList.iterator().next().getString(2).equals(" Russia]");
    }

    @Test
    public void testDifferentCountries() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString2));
        JavaRDD<Row> result = countCountries(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        System.out.println("-" + rowList.iterator().next().getString(0));
        System.out.println("-" + rowList.iterator().next().getString(1));
        System.out.println("-" + rowList.iterator().next().getString(2));
        assert rowList.iterator().next().getString(0).equals(" 18");
        assert rowList.iterator().next().getString(1).equals(" Russia");
        assert rowList.iterator().next().getString(2).equals(" Germany]");
    }

}
