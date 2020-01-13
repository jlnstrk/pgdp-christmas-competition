package pgdp.freiwillig;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatabaseTest {

    @BeforeAll
    static void init() {
        Database.setBaseDataDirectory(Paths.get("data"));
    }

    @Test
    void bigDataSetIntegrity() {
        Database database = new Database();

        // first line
        assertTrue(database.customers.get("BUILDING".hashCode()).contains(1));

        // last line
        assertTrue(database.customers.get("AUTOMOBILE".hashCode()).contains(150000));

        // first line
        assertTrue(database.orders.get(36901).contains(1));

        // last line
        assertTrue(database.orders.get(110063).contains(6000000));
    }

    /**
     * On Java 13, macOS 10.13.6 High Sierra, Intel Core i7-6770K non-oc'd, 2*16GB DDR4 2400 MHz
     * Using a bigger dataset (~1GB); 24.35MB (customer.tbl) + 171.95MB (orders.tbl) + 759.86MB (lineitem.tbl)
     * With jvm args '-Xms16G -Xmx24G' and following number of runs:
     * 1: avg ~1250ms, top ~1150ms
     * 5: avg ~900ms, top ~820ms
     * 20: avg ~650ms, top ~420ms
     * 50: avg ~550ms, top ~400ms
     * 100: avg ~420ms, top ~380ms
     */
    @ParameterizedTest
    @ValueSource(ints = { 1, 5, 20, 50, 100 })
    void performance(int runs) {
        Database.setBaseDataDirectory(Paths.get("data"));
        long[] quants = new long[5];
        String[] segments = new String[]{"FURNITURE", "HOUSEHOLD", "AUTOMOBILE", "BUILDING", "MACHINERY"};
        double fastestRun = Long.MAX_VALUE;
        double globalDuration = 0;
        for (int i = 0; i < runs; i++) {
            long preRun = System.nanoTime();
            Database db = new Database();
            for (int j = 0; j < segments.length; j++) {
                quants[j] = db.getAverageQuantityPerMarketSegment(segments[j]);
            }
            long postRun = System.nanoTime();
            double runDurationMs = (postRun - preRun) / Math.pow(10, 6);
            globalDuration += runDurationMs;
            System.out.println("run " + (i + 1) + ": " + String.format("%.2f", runDurationMs) + " ms");
            if (fastestRun > runDurationMs) {
                fastestRun = runDurationMs;
            }
        }
        for (int i = 0; i < quants.length; i++) {
            System.out.println("segment " + segments[i] + ": " + quants[i] + " avg");
        }
        System.out.printf(runs + " run(s) in: %.2f ms avg\n", globalDuration / runs);
        System.out.printf("fastest run: %.2f", fastestRun);
    }

}
