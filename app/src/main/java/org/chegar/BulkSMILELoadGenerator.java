package org.chegar;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;

// For OpenAI dataset, but maybe more.
public class BulkSMILELoadGenerator {

    static final HttpClient CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    // Precomputed length-prefixed Smile index action line, {"index":{}}
    private static final byte[] INDEX_ACTION_LINE = new byte[]{
            (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x0E, // 14 bytes
            (byte) 0x3A,(byte)  0x29, (byte) 0x0A, (byte) 0x01, // smile header
            (byte) 0xFA, // START_OBJECT (root)
            (byte) 0x84, 0x69, 0x6E, 0x64, 0x65, 0x78, // short field-name token (A4) + ASCII "index"
            (byte) 0xFA, (byte) 0xFB, // START_OBJECT (value) then END_OBJECT (empty object)
            (byte) 0xFB //END_OBJECT (root)
    };

    static final AtomicLong TOTAL_DOCS_SENT = new AtomicLong(0);
    private static final AtomicLong TOTAL_FAILED_BULKS = new AtomicLong(0);
    private static final AtomicLong TOTAL_BYTES_SENT = new AtomicLong(0);

    // Represent a byte range in the file
    public record ByteRange(long startByte, long endByte) {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: java BulkSMILELoadGenerator <esUrl> <indexName> <bulkSize> <indexingThreads> <filePath>");
            System.exit(1);
        }

        String esUrl = args[0];
        String indexName = args[1];
        int bulkSize = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        Path filePath = Path.of(args[4]);

        long fileSize = Files.size(filePath);
        List<ByteRange> ranges = partitionFileByBytes(filePath, numThreads);

        System.out.printf(
                "Starting load: fileSize=%,d bytes, threads=%d, bulkSize=%d, file=%s%n",
                fileSize, numThreads, bulkSize, filePath
        );
        for (int i = 0; i < ranges.size(); i++) {
            var r = ranges.get(i);
            System.out.printf("Thread-%d: byteStart=%,d byteEnd=%,d%n", i, r.startByte(), r.endByte());
        }

        CountDownLatch readyLatch = new CountDownLatch(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            ByteRange range = ranges.get(i);

            Thread t = new Thread(() -> {
                try {
                    processChunk(esUrl, indexName, filePath, range.startByte(), range.endByte(),
                            bulkSize, readyLatch, startLatch);
                } catch (Exception e) {
                    System.err.printf("Thread-%d failed: %s%n", threadId, e.getMessage());
                    e.printStackTrace();
                }
            }, "bulk-thread-" + threadId);
            threads.add(t);
        }

        // Start threads
        threads.forEach(Thread::start);

        // Wait until all are ready (skipped to start)
        readyLatch.await();

        // start the progress reporter
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(() ->
                        System.out.printf("Progress: %,d docs sent%n", TOTAL_DOCS_SENT.get()),
                5, 5, TimeUnit.SECONDS);

        System.out.println("All threads ready — releasing start latch!");
        long start = System.nanoTime();
        startLatch.countDown();

        // Wait for completion
        for (Thread t : threads) t.join();

        reporter.shutdownNow();

        double elapsedSec = (System.nanoTime() - start) / 1_000_000_000.0;
        printSummary(elapsedSec);
    }

    static void processChunk(String esUrl, String indexName, Path path,
                             long startByte, long endByte, int bulkSize,
                             CountDownLatch readyLatch, CountDownLatch startLatch)
            throws IOException, InterruptedException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
             InputStream in = Channels.newInputStream(channel);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8), 128 * 1024)) {

            // Seek to the start of our range
            channel.position(startByte);

            // If not at start, discard the first partial line
            if (startByte > 0) reader.readLine();

            readyLatch.countDown();
            startLatch.await();

            ByteArrayOutputStream baos = new ByteArrayOutputStream(16384);
            DataOutputStream out = new DataOutputStream(baos);
            int count = 0;
            long approxPos = startByte;

            for (int i = 0; i < range.docCount(); i++) {

            }

                while ((line = reader.readLine()) != null) {
                approxPos += line.getBytes(StandardCharsets.UTF_8).length + 1;
                if (approxPos >= endByte) break;

                // Write precomputed index action line
                baos.write(INDEX_ACTION_LINE);

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

//                baos.write(INDEX_ACTION_LINE);
//                baos.write(line.getBytes(UTF_8));
//                baos.write('\n');
//                count++;

                if (count == bulkSize) {
                    sendBulk(esUrl, indexName, baos.toByteArray(), count);
                    baos.reset();
                    count = 0;
                }
            }

            if (count > 0) {
                sendBulk(esUrl, indexName, baos.toByteArray(), count);
            }
        }
    }

    static void sendBulk(String esUrl, String indexName, byte[] body, int docCount)
            throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(esUrl + "/" + indexName + "/_bulk"))
                .header("Content-Type", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        TOTAL_BYTES_SENT.addAndGet(body.length);

        if (response.statusCode() >= 300) {
            TOTAL_FAILED_BULKS.incrementAndGet();
            System.err.printf("[%s] Bulk failed: %d%n", Thread.currentThread().getName(), response.statusCode());
        } else {
            TOTAL_DOCS_SENT.addAndGet(docCount);
        }
    }

    static List<ByteRange> partitionFileByBytes(Path path, int numThreads) throws IOException {
        long fileSize = Files.size(path);
        long chunkSize = fileSize / numThreads;

        List<ByteRange> list = new ArrayList<>(numThreads);
        long start = 0;
        for (int i = 0; i < numThreads; i++) {
            long end = (i == numThreads - 1) ? fileSize : start + chunkSize;
            list.add(new ByteRange(start, end));
            start = end;
        }
        return list;
    }

    static void printSummary(double elapsedSec) {
        System.out.println("\n=== Bulk Load Summary ===");
        System.out.printf("Total docs sent: %,d%n", TOTAL_DOCS_SENT.get());
        System.out.printf("Total failed bulks: %,d%n", TOTAL_FAILED_BULKS.get());
        System.out.printf("Total bytes sent: %,d (%.2f MB)%n",
                TOTAL_BYTES_SENT.get(), TOTAL_BYTES_SENT.get() / (1024.0 * 1024.0));
        System.out.printf("Elapsed time: %.2f sec%n", elapsedSec);
        System.out.printf("Average throughput: %.2f docs/sec%n",
                TOTAL_DOCS_SENT.get() / elapsedSec);
        System.out.println("========================");
    }
}
