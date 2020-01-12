package pgdp.freiwillig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Database {
    private static final int EXPECTED_NUM_CUSTOMERS_PER_SEGMENT = 30000;
    private static final int EXPECTED_NUM_ORDERS_PER_CUSTOMER = 15;
    private static final int EXPECTED_NUM_CUSTOMERS_WITH_ORDERS = 100000;
    private static final int EXPECTED_SIZE_LINE_ITEMS = 1500000;
    private static File TBL_CUSTOMER = null, TBL_LINE_ITEM = null, TBL_ORDERS = null;
    final Map<Integer, Collection<Integer>> customers = new ConcurrentHashMap<>();
    final Map<Integer, Collection<Integer>> orders = new ConcurrentHashMap<>((EXPECTED_NUM_CUSTOMERS_WITH_ORDERS * 4 + 2) / 3);
    final Map<Integer, long[]> lineItems = new ConcurrentHashMap<>((EXPECTED_SIZE_LINE_ITEMS * 4 + 2) / 3);
    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();
    private final ExecutorService startupExecutor = Executors.newFixedThreadPool(CPU_COUNT);

    interface ChunkProcessor {
        void process(byte[] src, int offset, int limit);
    }

    public static void setBaseDataDirectory(Path baseDirectory) {
        TBL_CUSTOMER = new File(baseDirectory.toString() +
                File.separator + "customer.tbl");
        TBL_LINE_ITEM = new File(baseDirectory.toString() +
                File.separator + "lineitem.tbl");
        TBL_ORDERS = new File(baseDirectory.toString() +
                File.separator + "orders.tbl");
    }

    public Database() {
        processFile(TBL_LINE_ITEM, this::processLineItemChunk);
        processFile(TBL_ORDERS, this::processOrderData);
        processFile(TBL_CUSTOMER, this::processCustomerData);
        startupExecutor.shutdown();
        try {
            startupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processFile(File file, ChunkProcessor processor) {
        try (FileChannel channel = new FileInputStream(file.getPath()).getChannel()) {
            int limit = 0;
            try {
                limit = (int) channel.size();
            } catch (IOException e) {
                e.printStackTrace();
            }
            int opSize = limit / CPU_COUNT;
            byte[] src = new byte[limit];
            for (int offset = 0; offset < limit; offset += opSize) {
                int stepSize = Math.min(opSize, limit - offset);
                try {
                    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, stepSize);
                    int finalOffset = offset;
                    startupExecutor.execute(() -> {
                        if (buffer != null) {
                            buffer.get(src, finalOffset, buffer.limit());
                        }
                        processor.process(src, finalOffset, finalOffset + stepSize);
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processCustomerData(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n' || offset == 0) {
                int postCustKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postCustKey == -1) break;
                int custKey = parseInt(src, offset + (offset == 0 ? 0 : 1), postCustKey - offset - (offset == 0 ? 0 : 1));
                int preSegment = byteArrayOrdinalIndexOf(src, (byte) '|', postCustKey + 1, 4);
                int postSegment = byteArrayIndexOf(src, (byte) '|', preSegment + 1);
                Integer segment = binaryStringHashCode(src, preSegment + 1, postSegment - preSegment - 1);
                Collection<Integer> set;
                if ((set = customers.get(segment)) == null) {
                    set = ConcurrentHashMap.newKeySet((EXPECTED_NUM_CUSTOMERS_PER_SEGMENT * 4 + 2) / 3);
                    customers.put(segment, set);
                }
                set.add(custKey);
                offset = postSegment + 32;
            }
        }
    }

    private void processOrderData(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n' || offset == 0) {
                int postOrderKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postOrderKey == -1) break;
                int orderKey = parseInt(src, offset + (offset == 0 ? 0 : 1), postOrderKey - offset - (offset == 0 ? 0 : 1));
                int postCustKey = byteArrayIndexOf(src, (byte) '|', postOrderKey + 1);
                Integer custKey = parseInt(src, postOrderKey + 1, postCustKey - postOrderKey - 1);
                Collection<Integer> deque;
                if ((deque = orders.get(custKey)) == null) {
                    deque = Collections.synchronizedCollection(new ArrayDeque<>(EXPECTED_NUM_ORDERS_PER_CUSTOMER));
                    orders.put(custKey, deque);
                }
                deque.add(orderKey);
                offset = postCustKey + 32;
            }
        }
    }

    private void processLineItemChunk(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n' || offset == 0) {
                int postOrderKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postOrderKey == -1) break;
                Integer orderKey = parseInt(src, offset + (offset == 0 ? 0 : 1), postOrderKey - offset - (offset == 0 ? 0 : 1));
                int sepPreQuantity = byteArrayOrdinalIndexOf(src, (byte) '|', postOrderKey + 1, 2);
                int sepPostQuantity = byteArrayIndexOf(src, (byte) '|', sepPreQuantity + 1);
                int quantity = 100 * parseInt(src, sepPreQuantity + 1, sepPostQuantity - sepPreQuantity - 1);
                long[] mem;
                if ((mem = lineItems.get(orderKey)) == null) {
                    mem = new long[2];
                    lineItems.put(orderKey, mem);
                }
                mem[0] = mem[0] + 1;
                mem[1] = mem[1] + quantity;
                // 64 (constants) is a safe skip
                offset = sepPostQuantity + 1 + 64;
            }
        }
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        final AtomicLong lineItemsCount = new AtomicLong();
        final AtomicLong totalQuantity = new AtomicLong();
        Collection<Integer> customers = this.customers.get(marketsegment.hashCode());
        List<ForkJoinTask<?>> tasks = new LinkedList<>();
        for (Integer customerKey : customers) {
            tasks.add(ForkJoinPool.commonPool().submit(() -> {
                Collection<Integer> orders = this.orders.get(customerKey);
                if (orders != null) {
                    for (Integer orderKey : orders) {
                        long[] quantities = lineItems.get(orderKey);
                        if (quantities != null) {
                            lineItemsCount.addAndGet(quantities[0]);
                            totalQuantity.addAndGet(quantities[1]);
                        }
                    }
                }
            }));
        }
        for (ForkJoinTask<?> task : tasks) {
            task.join();
        }
        long lineItemsCount_ = lineItemsCount.get();
        if (lineItemsCount_ == 0) {
            return -1;
        }
        return totalQuantity.get() / lineItemsCount_;
    }

    public static void main(String[] args) {
        Database.setBaseDataDirectory(Paths.get("data"));
        int runs = Integer.parseInt(args[0]);
        long[] quants = new long[5];
        String[] segments = new String[]{"FURNITURE", "HOUSEHOLD", "AUTOMOBILE", "BUILDING", "MACHINERY"};
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
        }
        for (int i = 0; i < quants.length; i++) {
            System.out.println("segment " + segments[i] + ": " + quants[i] + " avg");
        }
        System.out.printf(runs + " run(s) in: %.2f ms avg", globalDuration / runs);
    }

    public static int parseInt(byte[] src, int offset, int length) {
        int value = 0;
        length = offset + length - 1;
        for (int place = 1; length >= offset; length--, place *= 10) {
            value += (src[length] ^ 0x30) * place;
        }
        return value;
    }

    public static int byteArrayOrdinalIndexOf(byte[] src, byte of, int offset, int n) {
        int length = src.length;
        for (; offset < length; offset++) {
            byte b = src[offset];
            if (b == of) {
                if (n == 0) {
                    return offset;
                } else n--;
            }
        }
        return -1;
    }

    public static int byteArrayOrdinalLastIndexOf(byte[] src, byte of, int offset, int n) {
        for (; offset >= 0; offset--) {
            byte b = src[offset];
            if (b == of) {
                if (n == 0) {
                    return offset;
                } else n--;
            }
        }
        return -1;
    }

    public static int byteArrayIndexOf(byte[] src, byte of, int offset) {
        int length = src.length;
        for (; offset < length; offset++) {
            byte b = src[offset];
            if (b == of) {
                return offset;
            }
        }
        return -1;
    }

    public static int binaryStringHashCode(byte[] src, int offset, int length) {
        int h = 0;
        int max = length + offset;
        for (; offset < max; offset++) {
            h = 31 * h + src[offset];
        }
        return h;
    }

}
