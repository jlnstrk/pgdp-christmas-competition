package pgdp.freiwillig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class Database {
    private static File TBL_CUSTOMER = null, TBL_LINEITEM = null, TBL_ORDERS = null;

    public static void setBaseDataDirectory(Path baseDirectory) {
        TBL_CUSTOMER = new File(baseDirectory.toString() +
                File.separator + "customer.tbl");
        TBL_LINEITEM = new File(baseDirectory.toString() +
                File.separator + "lineitem.tbl");
        TBL_ORDERS = new File(baseDirectory.toString() +
                File.separator + "orders.tbl");
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        long averageQuantity = -1;
        BufferedReader customerReader = null, ordersReader = null, lineItemReader = null;
        try {
            customerReader = new BufferedReader(new FileReader(TBL_CUSTOMER));
            ordersReader = new BufferedReader(new FileReader(TBL_ORDERS));
            lineItemReader = new BufferedReader(new FileReader(TBL_LINEITEM));
            Set<Long> custKeys = new HashSet<>();
            String l1;
            while ((l1 = customerReader.readLine()) != null) {
                l1 = l1.substring(0, l1.length() - 2);
                int segmentLast = l1.lastIndexOf("|") - 1;
                int segmentFirst = l1.lastIndexOf("|", segmentLast) + 1;
                String segment = l1.substring(segmentFirst, segmentLast + 1);
                if (segment.equals(marketsegment)) {
                    long custKey = Long.parseUnsignedLong(l1, 0, l1.indexOf("|"), 10);
                    custKeys.add(custKey);
                }
            }
            Set<Long> orderKeys = new HashSet<>();

            String l2;
            while ((l2 = ordersReader.readLine()) != null) {
                int custKeyFirst = l2.indexOf("|") + 1;
                int custKeyLast = l2.indexOf("|", custKeyFirst) - 1;
                long custKey = Long.parseUnsignedLong(l2, custKeyFirst, custKeyLast + 1, 10);
                if (custKeys.contains(custKey)) {
                    long orderKey = Long.parseUnsignedLong(l2, 0, custKeyFirst - 1, 10);
                    orderKeys.add(orderKey);
                }
            }
            int orders = 0;
            long totalQuant = 0;
            String l3;
            long currKey = 0;
            while ((l3 = lineItemReader.readLine()) != null) {
                int orderKeyLast = l3.indexOf("|") - 1;
                long orderKey = Long.parseUnsignedLong(l3, 0, orderKeyLast + 1, 10);
                if (currKey == orderKey || orderKeys.contains(orderKey)) {
                    currKey = orderKey;
                    int quantityFirst = ordinalIndexOf(l3, "|", 4, orderKeyLast + 1) + 1;
                    int quantityLast = l3.indexOf("|", quantityFirst) - 1;
                    long quantity = 100 * Long.parseUnsignedLong(l3, quantityFirst, quantityLast + 1, 10);
                    orders++;
                    totalQuant += quantity;
                }
            }
            averageQuantity = totalQuant / orders;
        } catch (Exception e) {
            // ignore
        } finally {
            try {
                if (customerReader != null) {
                    customerReader.close();
                }
                if (ordersReader != null) {
                    ordersReader.close();
                }
                if (lineItemReader != null) {
                    lineItemReader.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return averageQuantity;
    }

    public static void main(String[] args) {
        Database.setBaseDataDirectory(Paths.get("data"));
        Database db = new Database();
        long before = System.nanoTime();
        long qt = db.getAverageQuantityPerMarketSegment("AUTOMOBILE");
        long after = System.nanoTime();
        double result = (after - before) / Math.pow(10, 6);
        System.out.println(result);
        System.out.println(qt);
    }

    public static int ordinalIndexOf(String str, String substr, int n, int offset) {
        int pos = str.indexOf(substr, offset);
        while (--n > 0 && pos != -1)
            pos = str.indexOf(substr, pos + 1);
        return pos;
    }

}
