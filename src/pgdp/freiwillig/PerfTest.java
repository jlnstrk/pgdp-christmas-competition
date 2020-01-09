package pgdp.freiwillig;

import java.util.StringTokenizer;

public class PerfTest {
    private static final String PERF_SAMPLE = "4|88035|5560|1|30|30690.90|0.03|0.08|N|O|1996-01-10|1995-12-14|1996-01-18|DELIVER IN PERSON|REG AIR|- quickly regular packages sleep. idly|";

    private static long measure(Runnable runnable) {
        long before = System.nanoTime();
        runnable.run();
        long after = System.nanoTime();
        return after - before;
    }

    public static void main(String[] args) {
        System.out.println(Database.parseLong(new byte[] { '1', '2', '3' }, 1, 2));

        long indexOf = 0, split = 0, split_limit = 0, tokenizer = 0, bytewise = 0;
        for (int i = 0; i < 1000000; i++) {
            indexOf += measure(() -> {
                int orderKeyLast = PERF_SAMPLE.indexOf("|") - 1;
                long orderKey = Long.parseUnsignedLong(PERF_SAMPLE, 0, orderKeyLast + 1, 10);
                int sepFront = Database.ordinalIndexOf(PERF_SAMPLE, "|", 4, orderKeyLast + 1);
                int sepBack = PERF_SAMPLE.indexOf("|", sepFront + 1);
                long quantity = 100 * Long.parseUnsignedLong(PERF_SAMPLE, sepFront + 1, sepBack, 10);
            });
            split += measure(() -> {
                String[] segments = PERF_SAMPLE.split("\\|");
                long orderKey = Long.parseLong(segments[0]);
                long quantity = 100 * Long.parseLong(segments[4]);
            });
            split_limit += measure(() -> {
                String[] segments = PERF_SAMPLE.split("\\|", 6);
                long orderKey = Long.parseLong(segments[0]);
                long quantity = 100 * Long.parseLong(segments[4]);
            });
            tokenizer += measure(() -> {
                StringTokenizer tkn = new StringTokenizer(PERF_SAMPLE, "|", false);
                long orderKey = Long.parseLong(tkn.nextToken());
                for (int j = 0; j < 3; j++) {
                    tkn.nextToken();
                }
                long quantity = Long.parseLong(tkn.nextToken());
            });
            bytewise += measure(() -> {
                byte[] bytes = PERF_SAMPLE.getBytes();
                long orderKey;
                long quantity;
                for (int i1 = 0, occ = 0, prev = 0; i1 < bytes.length; i1++) {
                    if (bytes[i1] == '|') {
                        if (occ == 0) {
                            orderKey = Long.parseLong(new String(bytes, 0, i1));
                        } else if (occ == 4) {
                            quantity = Long.parseLong(new String(bytes, prev + 1, i1 - prev - 1));
                            break;
                        }
                        occ++;
                        prev = i1;
                    }
                }
            });
        }
        System.out.println("indexOf: " + (indexOf / Math.pow(10, 6))
                + ", split: " + (split / Math.pow(10, 6))
                + ", split_limit: " + (split_limit / Math.pow(10, 6))
                + ", tokenizer: " + (tokenizer / Math.pow(10, 6))
                + ", bytewise: " + (bytewise / Math.pow(10, 6)));
    }

}
