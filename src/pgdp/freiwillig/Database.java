package pgdp.freiwillig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Database {
    private static File TBL_CUSTOMER = null, TBL_LINE_ITEM = null, TBL_ORDERS = null;
    private Map<String, Set<Long>> customers = new HashMap<>();
    private Map<Long, Set<Long>> orders = new HashMap<>();
    private Map<Long, Set<Long>> lineItems = new HashMap<>();

    public static void setBaseDataDirectory(Path baseDirectory) {
        TBL_CUSTOMER = new File(baseDirectory.toString() +
                File.separator + "customer.tbl");
        TBL_LINE_ITEM = new File(baseDirectory.toString() +
                File.separator + "lineitem.tbl");
        TBL_ORDERS = new File(baseDirectory.toString() +
                File.separator + "orders.tbl");
    }

    public Database() {
        parseCustomerData();
        parseOrdersData();
        parseLineItemsData();
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
                    customers.computeIfAbsent(key, k -> new HashSet<>())
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
                    orders.computeIfAbsent(custKey, k -> new HashSet<>())
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
                    lineItems.computeIfAbsent(orderKey, k -> new HashSet<>())
                            .add(quantity);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        long lineItemsCount = 0;
        long totalQuantity = 0;
        Set<Long> sgmtCustomers = customers.get(marketsegment);
        for (Long custKey : sgmtCustomers) {
            Set<Long> orderKeys = orders.get(custKey);
            if (orderKeys != null) {
                for (Long orderKey : orderKeys) {
                    Set<Long> quantities = lineItems.get(orderKey);
                    lineItemsCount += quantities.size();
                    for (Long quantity : quantities) {
                        totalQuantity += quantity;
                    }
                }
            }
        }
        return totalQuantity / lineItemsCount;
    }

    public static void main(String[] args) {
        Database.setBaseDataDirectory(Paths.get("data"));
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
        long totalDur = 0;
        for (int i = 0; i < segments.length; i++) {
            totalDur += durs[i];
            System.out.println(segments[i] + ": average " + quants[i] + " took " + durs[i] + "ms");
        }
        System.out.println("total average duration: " + (totalDur / durs.length) + "ms");
    }

    public static int ordinalIndexOf(String str, String substr, int n, int offset) {
        int pos = str.indexOf(substr, offset);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(substr, pos + 1);
        return pos;
    }

}
