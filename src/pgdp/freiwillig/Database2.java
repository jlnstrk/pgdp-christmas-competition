package pgdp.freiwillig;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class Database2 {
    private static File TBL_CUSTOMER = null, TBL_LINE_ITEM = null, TBL_ORDERS = null;
    private Map<String, Set<Long>> customers = new ConcurrentHashMap<>();
    private Map<Long, Set<Long>> orders = new ConcurrentHashMap<>();
    private Map<Long, Set<Long>> lineItems = new ConcurrentHashMap<>();
    private final ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    public static void setBaseDataDirectory(Path baseDirectory) {
        TBL_CUSTOMER = new File(baseDirectory.toString() +
                File.separator + "customer.tbl");
        TBL_LINE_ITEM = new File(baseDirectory.toString() +
                File.separator + "lineitem.tbl");
        TBL_ORDERS = new File(baseDirectory.toString() +
                File.separator + "orders.tbl");
    }

    public Database2() {
        processFile(TBL_LINE_ITEM, this::processLineItemData);
        processFile(TBL_ORDERS, this::processOrderData);
        processFile(TBL_CUSTOMER, this::processCustomerData);
        service.shutdown();
        try {
            service.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(customers.keySet());
    }

    private void processFile(File file, Consumer<byte[]> processor) {
        byte[] all = null;
        List<Future<?>> tasks = new LinkedList<>();
        try (FileChannel channel = new FileInputStream(file.getPath()).getChannel()) {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            all = new byte[buffer.limit()];
            int opSize = 1 << 19;
            int opCount = buffer.limit() / opSize;
            for (int i = 0, off = 0; i < opCount; i++, off += opSize) {
                int fOff = off;
                byte[] finalAll = all;
                tasks.add(service.submit(() -> {
                    ByteBuffer view = buffer.slice(fOff, opSize);
                    view.get(finalAll, fOff, opSize);
                }));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            for (Future<?> task : tasks) {
                try {
                    task.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        processor.accept(all);
    }

    private void processCustomerData(byte[] data) {
        long custKey = 0;
        int length = data.length;
        for (int i = 0, prevSep = 0, iSep = 0; i < length; i++) {
            if (data[i] == '|') {
                // separator after orderKey
                if (iSep % 8 == 0) {
                    custKey = 0;
                    for (int j = i - 1, place = 1; j >= prevSep + (i == 0 ? 0 : 2); j--, place *= 10) {
                        custKey += (data[j] ^ 0x30) * place;
                    }
                }
                // separator after custKey
                if ((iSep + 2) % 8 == 0) {
                    int finalPrevSep = prevSep;
                    long finalCustKey = custKey;
                    int finalI = i;
                    service.execute(() -> {
                        String segment = new String(data, finalPrevSep + 1, finalI - finalPrevSep - 1, StandardCharsets.UTF_8);
                        customers.computeIfAbsent(segment, k -> new HashSet<>())
                                .add(finalCustKey);
                    });
                }
                prevSep = i;
                iSep++;
            }
        }
    }

    private void processOrderData(byte[] data) {
        long orderKey = 0;
        int length = data.length;
        for (int i = 0, prevSep = 0, iSep = 0; i < length; i++) {
            if (data[i] == '|') {
                // separator after orderKey
                if (iSep % 9 == 0) {
                    orderKey = 0;
                    for (int j = i - 1, place = 1; j >= prevSep + (i == 0 ? 0 : 2); j--, place *= 10) {
                        orderKey += (data[j] ^ 0x30) * place;
                    }
                }
                // separator after custKey
                if ((iSep - 1) % 9 == 0) {
                    int finalPrevSep = prevSep;
                    long finalOrderKey = orderKey;
                    int finalI = i;
                    service.execute(() -> {
                        long custKey = 0;
                        for (int j = finalI - 1, place = 1; j >= finalPrevSep + 1; j--, place *= 10) {
                            custKey += (data[j] ^ 0x30) * place;
                        }
                        orders.computeIfAbsent(custKey, k -> new HashSet<>())
                                .add(finalOrderKey);
                    });
                }
                prevSep = i;
                iSep++;
            }
        }
    }

    private void processLineItemData(byte[] data) {
        System.out.println("processing...");
        long orderKey = 0;
        int length = data.length;
        for (int i = 0, prevSep = 0, iSep = 0; i < length; i++) {
            if (data[i] == '|') {
                // separator after orderKey
                if (iSep % 16 == 0) {
                    orderKey = 0;
                    for (int j = i - 1, place = 1; j >= prevSep + (i == 0 ? 0 : 2); j--, place *= 10) {
                        orderKey += (data[j] ^ 0x30) * place;
                    }
                }
                // separator after quantity
                if ((iSep - 4) % 16 == 0) {
                    long finalOrderKey = orderKey;
                    int finalPrevSep = prevSep;
                    int finalI = i;
                    service.execute(() -> {
                        long quantity = 0;
                        for (int j = finalI - 1, place = 1; j >= finalPrevSep + 1; j--, place *= 10) {
                            quantity += (data[j] ^ 0x30) * place;
                        }
                        quantity *= 100;
                        lineItems.computeIfAbsent(finalOrderKey, k -> new HashSet<>())
                                .add(quantity);
                    });
                }
                prevSep = i;
                iSep++;
            }
        }
        System.out.println("end of loop");
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        final AtomicLong lineItemsCount = new AtomicLong();
        final AtomicLong totalQuantity = new AtomicLong();
        Set<Long> sgmtCustomers = customers.get(marketsegment);
        List<Future<?>> futures = new LinkedList<>();
        for (Long custKey : sgmtCustomers) {
            futures.add(ForkJoinPool.commonPool().submit(() -> {
                Set<Long> orderKeys = orders.get(custKey);
                if (orderKeys != null) {
                    for (Long orderKey : orderKeys) {
                        Set<Long> quantities = lineItems.get(orderKey);
                        if (quantities != null) {
                            lineItemsCount.addAndGet(quantities.size());
                            for (Long quantity : quantities) {
                                totalQuantity.addAndGet(quantity);
                            }
                        }
                    }
                }
            }));
        }
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return totalQuantity.get() / lineItemsCount.get();
    }

    public static void main(String[] args) {
        Database2.setBaseDataDirectory(Paths.get("data"));
        long globalBefore = System.nanoTime();
        Database2 db = new Database2();
        int[] durs = new int[5];
        long[] quants = new long[5];
        String[] segments = new String[]{"FURNITURE", "HOUSEHOLD", "AUTOMOBILE", "BUILDING", "MACHINERY"};
        for (int i = 0; i < segments.length; i++) {
            long before = System.nanoTime();
            long qt = db.getAverageQuantityPerMarketSegment(segments[i]);
            long after = System.nanoTime();
            durs[i] = (int) ((after - before) / Math.pow(10, 6));
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

    public static int parseInt(byte[] from, int begin, int length) {
        int value = 0;
        for (int i = begin + length - 1, pos = 1; i >= begin; i--, pos *= 10) {
            value += (from[i] ^ 0x30) * pos;
        }
        return value;
    }

}
