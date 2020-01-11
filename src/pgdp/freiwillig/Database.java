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
import java.util.stream.Collectors;

public class Database {
    private int CPU_COUNT = 2;
    private static File TBL_CUSTOMER = null, TBL_LINE_ITEM = null, TBL_ORDERS = null;
    private Map<Integer, Collection<Integer>> customers = new ConcurrentHashMap<>();
    private Map<Integer, Collection<Integer>> orders = new ConcurrentHashMap<>();
    private Map<Integer, long[]> lineItems = new ConcurrentHashMap<>();
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
        CPU_COUNT = Math.max(Runtime.getRuntime().availableProcessors(), 2);
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
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            int opSize = buffer.limit() / CPU_COUNT;
            byte[] buf = new byte[buffer.limit()];
            while (buffer.hasRemaining()) {
                int position = buffer.position();
                buffer.get(buf, position, Math.min(buffer.remaining(), opSize));
                startupExecutor.execute(() -> {
                    int limit = position + opSize;
                    processor.process(buf, position, Math.min(limit, buf.length));
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processCustomerData(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n') {
                int postCustKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postCustKey == -1) {
                    break;
                }
                int custKey = parseInt(src, offset + 1, postCustKey - offset - 1);
                int preSegment = byteArrayOrdinalIndexOf(src, (byte) '|', postCustKey + 1, 4);
                int postSegment = byteArrayIndexOf(src, (byte) '|', preSegment + 1);
                int segment = binaryStringHashCode(src, preSegment + 1, postSegment - preSegment - 1);
                customers.computeIfAbsent(segment, k -> new ConcurrentLinkedDeque<>())
                        .add(custKey);
                offset = postSegment + 1;
            }
        }
    }

    private void processOrderData(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n') {
                int postOrderKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postOrderKey == -1) {
                    break;
                }
                int orderKey = parseInt(src, offset + 1, postOrderKey - offset - 1);
                int postCustKey = byteArrayIndexOf(src, (byte) '|', postOrderKey + 1);
                int custKey = parseInt(src, postOrderKey + 1, postCustKey - postOrderKey - 1);
                orders.computeIfAbsent(custKey, k -> Collections.synchronizedCollection(new ArrayDeque<>()))
                        .add(orderKey);
                offset = postCustKey + 1;
            }
        }
    }

    private void processLineItemChunk(byte[] src, int offset, int limit) {
        for (; offset < limit; offset++) {
            byte b = src[offset];
            if (b == '\n') {
                int postOrderKey = byteArrayIndexOf(src, (byte) '|', offset + 1);
                if (postOrderKey == -1) {
                    break;
                }
                int orderKey = parseInt(src, offset + 1, postOrderKey - offset - 1);
                int sepPreQuantity = byteArrayOrdinalIndexOf(src, (byte) '|', postOrderKey + 1, 2);
                int sepPostQuantity = byteArrayIndexOf(src, (byte) '|', sepPreQuantity + 1);
                int quantity = 100 * parseInt(src, sepPreQuantity + 1, sepPostQuantity - sepPreQuantity - 1);
                long[] mem = lineItems.computeIfAbsent(orderKey, k -> new long[2]);
                mem[0] = mem[0] + 1;
                mem[1] = mem[1] + quantity;
                offset = sepPostQuantity + 1;
            }
        }
    }

    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        final AtomicLong lineItemsCount = new AtomicLong();
        final AtomicLong totalQuantity = new AtomicLong();
        Collection<Integer> customers = this.customers.get(marketsegment.hashCode());
        List<Future<?>> futures = new LinkedList<>();
        for (Integer customerKey : customers) {
            futures.add(ForkJoinPool.commonPool().submit(() -> {
                Collection<Integer> orders = this.orders.get(customerKey);
                if (orders != null) {
                    for (Integer orderKey : orders) {
                        long[] quantities = lineItems.get(orderKey);
                        lineItemsCount.addAndGet(quantities[0]);
                        totalQuantity.addAndGet(quantities[1]);
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
        long lineItemsCount_ = lineItemsCount.get();
        if (lineItemsCount_ == 0) {
            return Long.parseLong(marketsegment.chars()
                    .mapToObj(String::valueOf)
                    .map(s -> (String) s)
                    .collect(Collectors.joining("0")));
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
