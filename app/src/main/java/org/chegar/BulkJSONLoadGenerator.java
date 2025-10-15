package org.chegar;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;

// For OpenAO dataset, but maybe more.
public class BulkJSONLoadGenerator {

    static final HttpClient CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    static final byte[] INDEX_LINE = "{\"index\":{}}\n".getBytes(UTF_8);
    static final AtomicLong TOTAL_DOCS_SENT = new AtomicLong(0);
    private static final AtomicLong TOTAL_FAILED_BULKS = new AtomicLong(0);
    private static final AtomicLong TOTAL_BYTES_SENT = new AtomicLong(0);

    // Record for bulk range
    public record BulkRange(long startLine, long lineCount) {}

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: java BulkJSONLoadGenerator <esUrl> <indexName> <bulkSize> <indexingThreads> <filePath>");
            System.exit(1);
        }

        String esUrl = args[0];
        String indexName = args[1];
        int bulkSize = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);
        Path filePath = Path.of(args[4]);

        long totalDocs = countLines(filePath);
        List<BulkRange> ranges = partitionLines(totalDocs, numThreads);

        System.out.printf(
                "Starting load: totalDocs=%,d, threads=%d, bulkSize=%d, file=%s%n",
                totalDocs, numThreads, bulkSize, filePath
        );
        ranges.forEach(r -> System.out.printf("Range start=%d count=%d%n", r.startLine(), r.lineCount()));

        CountDownLatch readyLatch = new CountDownLatch(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            BulkRange range = ranges.get(i);

            Thread t = new Thread(() -> {
                try {
                    processChunk(esUrl, indexName, filePath, range.startLine(), range.lineCount(),
                            bulkSize, readyLatch, startLatch);
                } catch (Exception e) {
                    System.err.printf("Thread-%d failed: %s%n", threadId, e.getMessage());
                    e.printStackTrace();
                }
            }, "bulk-thread-" + threadId);
            threads.add(t);
        }

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(() ->
                        System.out.printf("Progress: %,d docs sent%n", TOTAL_DOCS_SENT.get()),
                5, 5, TimeUnit.SECONDS);

        // Start threads
        threads.forEach(Thread::start);

        // Wait until all are ready (skipped to start)
        readyLatch.await();
        System.out.println("All threads ready — releasing start latch!");
        long start = System.nanoTime();
        startLatch.countDown();

        // Wait for completion
        for (Thread t : threads) t.join();

        reporter.shutdownNow();

        double elapsedSec = (System.nanoTime() - start) / 1_000_000_000.0;
        printSummary(totalDocs, elapsedSec);
    }

    static void processChunk(String esUrl, String indexName, Path path,
                                     long startLine, long docCount, int bulkSize,
                                     CountDownLatch readyLatch, CountDownLatch startLatch)
            throws IOException, InterruptedException {

        try (BufferedReader reader = Files.newBufferedReader(path, UTF_8)) {

            // Skip lines until we reach our starting point
            for (long i = 0; i < startLine; i++) {
                if (reader.readLine() == null)
                    throw new EOFException("File ended before startLine " + startLine);
            }

            // Signal ready
            readyLatch.countDown();

            // Wait for latch release (all threads aligned)
            startLatch.await();

            ByteArrayOutputStream baos = new ByteArrayOutputStream(16384);
            String line;
            int count = 0;

            while ((line = reader.readLine()) != null && docCount-- > 0) {
                baos.write(INDEX_LINE);
                baos.write(line.getBytes(UTF_8));
                baos.write('\n');
                count++;

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

    static long countLines(Path path) throws IOException {
        try (var lines = Files.lines(path)) {
            return lines.count();
        }
    }

    static List<BulkRange> partitionLines(long totalLines, int numThreads) {
        long perThread = totalLines / numThreads;
        long remainder = totalLines % numThreads;
        List<BulkRange> list = new ArrayList<>(numThreads);
        long current = 0;
        for (int i = 0; i < numThreads; i++) {
            long count = perThread + (i == numThreads - 1 ? remainder : 0);
            list.add(new BulkRange(current, count));
            current += count;
        }
        return list;
    }

    static void printSummary(long totalDocs, double elapsedSec) {
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
