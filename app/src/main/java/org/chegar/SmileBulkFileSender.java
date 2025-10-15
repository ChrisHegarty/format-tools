package org.chegar;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SmileBulkFileSender {

    // Precomputed length-prefixed Smile index action line, {"index":{}}
    private static final byte[] INDEX_ACTION_LINE = new byte[]{
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0E, // 14 bytes
            (byte) 0x3A,(byte)  0x29, (byte) 0x0A, (byte) 0x01, // smile header
            (byte) 0xFA, // START_OBJECT (root)
            (byte) 0x84, 0x69, 0x6E, 0x64, 0x65, 0x78, // short field-name token (A4) + ASCII "index"
            (byte) 0xFA, (byte) 0xFB, // START_OBJECT (value) then END_OBJECT (empty object)
            (byte) 0xFB //END_OBJECT (root)
    };

    // JSON: {"create":{}} Smile: 3A 29 0A 01 FA 85 63 72 65 61 74 65 FA FB FB
    private static final byte[] CREATE_ACTION_LINE = new byte[]{
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0F, // 15 bytes
            (byte) 0x3A,(byte)  0x29, (byte) 0x0A, (byte) 0x01, // smile header
            (byte) 0xFA, // START_OBJECT (root)
            (byte) 0x85,  0x63, 0x72, 0x65, 0x61, 0x74, 0x65, // short field-name token ASCII "create"
            (byte) 0xFA, (byte) 0xFB, // START_OBJECT (value) then END_OBJECT (empty object)
            (byte) 0xFB //END_OBJECT (root)
    };

    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final AtomicLong SENT_DOCS = new AtomicLong(0);

    // Record for bulk range
    public record BulkRange(long startOffset, int docCount) {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: java SmileBulkFileSender <esUrl> <indexName> <bulkSize> <filePath> <ds>");
            System.exit(1);
        }

        String esUrl = args[0];
        String indexName = args[1];
        int bulkSize = Integer.parseInt(args[2]);
        String filePath = args[3];
        boolean isForDataStream = Boolean.valueOf(args[4]);

        // Generate all bulk ranges in main thread
        List<BulkRange> bulkRanges = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            long pos = 0;
            while (pos < raf.length()) {
                long bulkStart = pos;
                int count = 0;
                while (pos < raf.length() && count < bulkSize) {
                    raf.seek(pos);
                    int len = raf.readInt();
                    pos += 4L + len;
                    count++;
                }
                bulkRanges.add(new BulkRange(bulkStart, count));
            }
        }

        System.out.println("Total bulk requests: " + bulkRanges.size());
        System.out.println("isForDataStream: " + isForDataStream);
        final byte[] action = isForDataStream ? CREATE_ACTION_LINE : INDEX_ACTION_LINE;

        // Divide contiguous blocks of bulk ranges among threads
        int numThreads = 8;
        List<List<BulkRange>> threadRanges = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) threadRanges.add(new ArrayList<>());

        int rangesPerThread = bulkRanges.size() / numThreads;
        int remainder = bulkRanges.size() % numThreads;

        int start = 0;
        for (int t = 0; t < numThreads; t++) {
            int end = start + rangesPerThread + (t < remainder ? 1 : 0);
            threadRanges.get(t).addAll(bulkRanges.subList(start, end));
            start = end;
        }

        // Start periodic progress reporter
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(() -> {
            System.out.printf("Documents sent: %d%n", SENT_DOCS.get());
        }, 5, 5, TimeUnit.SECONDS);

        // Start threads
        long startNanos = System.nanoTime();
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            List<BulkRange> rangesForThread = threadRanges.get(t);
            Thread thread = new Thread(() -> {
                try (RandomAccessFile raf = new RandomAccessFile(filePath, "r");
                     FileChannel channel = raf.getChannel()) {
                    for (BulkRange range : rangesForThread) {
                        sendBulk(channel, range, esUrl, indexName, action);
                        SENT_DOCS.addAndGet(range.docCount());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            threads.add(thread);
        }

        // Wait for threads to finish
        for (Thread t : threads) t.join();
        long elapsed = (System.nanoTime() - startNanos) / 1_000_000_000;
        reporter.shutdownNow();

        System.out.println("All documents sent. " + elapsed + "secs");
    }

    private static void sendBulk(FileChannel channel, BulkRange range,
                                 String esUrl, String indexName, byte[] action)
            throws IOException, InterruptedException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        long pos = range.startOffset();
        for (int i = 0; i < range.docCount(); i++) {
            // Write precomputed index action line
            out.write(action);

            // Read 4-byte length
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, pos);
            lenBuf.flip();
            int docLen = lenBuf.getInt();

            // Read Smile document
            ByteBuffer docBuf = ByteBuffer.allocate(docLen);
            channel.read(docBuf, pos + 4);
            docBuf.flip();

            // Write length + document
            out.writeInt(docLen);
            out.write(docBuf.array());

            pos += 4L + docLen;
        }

        // Send bulk request
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(esUrl + "/" + indexName + "/_bulk"))
                .header("Content-Type", "application/smile")
                .header("Bulk-Format", "prefix-length")
                .POST(HttpRequest.BodyPublishers.ofByteArray(baos.toByteArray()))
                .build();

        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            System.err.println("Bulk request failed: " + response.statusCode());
            System.err.println(response.body());
        }
    }
}
