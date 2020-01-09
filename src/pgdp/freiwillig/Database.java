package pgdp.freiwillig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Database {
    private static File TBL_CUSTOMER = null, TBL_LINE_ITEM = null, TBL_ORDERS = null;
    private Map<String, Set<Long>> customers = new ConcurrentHashMap<>();
    private Map<Long, Set<Long>> orders = new ConcurrentHashMap<>();
    private Map<Long, Set<Long>> lineItems = new ConcurrentHashMap<>();

    public static void setBaseDataDirectory(Path baseDirectory) {
        TBL_CUSTOMER = new File(baseDirectory.toString() +
                File.separator + "customer.tbl");
        TBL_LINE_ITEM = new File(baseDirectory.toString() +
                File.separator + "lineitem.tbl");
        TBL_ORDERS = new File(baseDirectory.toString() +
                File.separator + "orders.tbl");
    }

    public Database() {
        ForkJoinPool pool = new ForkJoinPool(3);
        ForkJoinTask<?> customers = pool.submit(this::parseCustomerData);
        ForkJoinTask<?> orders = pool.submit(this::parseOrdersData);
        ForkJoinTask<?> lineItems = pool.submit(this::parseLineItemsData);
        customers.join();
        orders.join();
        lineItems.join();
    }

    private void parseCustomerData() {
        try (FileReader tblReader = new FileReader(TBL_CUSTOMER)) {
            try (BufferedReader customerReader = new BufferedReader(tblReader)) {
                String l1;
                while ((l1 = customerReader.readLine()) != null) {
                    int segmentSepEnd = l1.lastIndexOf('|', l1.length() - 2);
                    int segmentSepStart = l1.lastIndexOf('|', segmentSepEnd - 1);
                    long custKey = Long.parseUnsignedLong(l1, 0, l1.indexOf("|"), 10);
                    String key = l1.substring(segmentSepStart + 1, segmentSepEnd);
                    customers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet())
                            .add(custKey);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseOrdersData() {
        try (FileReader tblReader = new FileReader(TBL_ORDERS)) {
            try (BufferedReader ordersReader = new BufferedReader(tblReader)) {
                String l2;
                while ((l2 = ordersReader.readLine()) != null) {
                    int custKeyFirst = l2.indexOf("|") + 1;
                    int custKeyLast = l2.indexOf("|", custKeyFirst) - 1;
                    long custKey = Long.parseUnsignedLong(l2, custKeyFirst, custKeyLast + 1, 10);
                    long orderKey = Long.parseUnsignedLong(l2, 0, custKeyFirst - 1, 10);
                    orders.computeIfAbsent(custKey, k -> ConcurrentHashMap.newKeySet())
                            .add(orderKey);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseLineItemsData() {
        try (FileReader tblReader = new FileReader(TBL_LINE_ITEM)) {
            try (BufferedReader lineItemReader = new BufferedReader(tblReader)) {
                String l3;
                while ((l3 = lineItemReader.readLine()) != null) {
                    int orderKeyLast = l3.indexOf("|") - 1;
                    long orderKey = Long.parseUnsignedLong(l3, 0, orderKeyLast + 1, 10);
                    int sepFront = ordinalIndexOf(l3, "|", 4, orderKeyLast + 1);
                    int sepBack = l3.indexOf("|", sepFront + 1);
                    long quantity = 100 * Long.parseUnsignedLong(l3, sepFront + 1, sepBack, 10);
                    lineItems.computeIfAbsent(orderKey, k -> ConcurrentHashMap.newKeySet())
                            .add(quantity);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseLineItemsDataAlternative() {

    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        final AtomicLong lineItemsCount = new AtomicLong();
        final AtomicLong totalQuantity = new AtomicLong();
        ExecutorService exec = Executors.newFixedThreadPool(16);
        Set<Long> sgmtCustomers = customers.get(marketsegment);
        for (Long custKey : sgmtCustomers) {
            exec.execute(() -> {
                Set<Long> orderKeys = orders.get(custKey);
                if (orderKeys != null) {
                    for (Long orderKey : orderKeys) {
                        Set<Long> quantities = lineItems.get(orderKey);
                        lineItemsCount.addAndGet(quantities.size());
                        for (Long quantity : quantities) {
                            totalQuantity.addAndGet(quantity);
                        }
                    }
                }
            });
        }
        try {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return totalQuantity.get() / lineItemsCount.get();
    }

    public static void main(String[] args) {
        Database.setBaseDataDirectory(Paths.get("data"));
        long globalBefore = System.nanoTime();
        Database db = new Database();
        long[] durs = new long[5];
        long[] quants = new long[5];
        String[] segments = new String[]{"FURNITURE", "HOUSEHOLD", "AUTOMOBILE", "BUILDING", "MACHINERY"};
        for (int i = 0; i < segments.length; i++) {
            long before = System.nanoTime();
            long qt = db.getAverageQuantityPerMarketSegment(segments[i]);
            long after = System.nanoTime();
            durs[i] = (long) ((after - before) / Math.pow(10, 6));
            quants[i] = qt;
        }
        long globalAfter = System.nanoTime();
        long totalDur = 0;
        for (int i = 0; i < segments.length; i++) {
            totalDur += durs[i];
            System.out.println(segments[i] + ": average " + quants[i] + " took " + durs[i] + "ms");
        }
        System.out.println("total duration: " + (globalAfter - globalBefore) / Math.pow(10, 6));
        System.out.println("total average duration: " + (totalDur / durs.length) + "ms");
    }

    public static int ordinalIndexOf(String str, String substr, int n, int offset) {
        int pos = str.indexOf(substr, offset);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(substr, pos + 1);
        return pos;
    }

    public static long parseLong(byte[] from, int begin, int length) {
        long value = 0;
        for (int i = begin + length - 1, pos = 1; i >= begin; i--, pos *= 10) {
            value += (from[i] ^ 0x30) * pos;
        }
        return value;
    }

}
