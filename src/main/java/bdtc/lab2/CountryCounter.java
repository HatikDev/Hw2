package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

@AllArgsConstructor
@Slf4j
public class CountryCounter {

    // Logs time format is HH:mm

    /**
     * Function for parsing input data and form result
     * @param inputDataset - input data for analyse
     * @return result in JavaRDD format
     */
    public static JavaRDD<Row> countCountries(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<LogLevelHour> logLevelHourDataset = words.map(s -> {
                    String[] logFields = s.split(",");
                    return new LogLevelHour((logFields[1].split(":"))[0], logFields[2], logFields[3]);
                }, Encoders.bean(LogLevelHour.class))
                .coalesce(1);

        // Group by hours and countries
        Dataset<Row> t = logLevelHourDataset.groupBy("hour", "countryin", "countryout")
                .count()
                .toDF("hour", "countryin", "countryout", "count")
                // sort by the hours
                .sort(functions.asc("hour"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
