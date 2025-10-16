package org.chegar;

import java.io.*;
import java.nio.file.*;

public class FileSplitter {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java StreamingFileSplitter <inputFile> <numParts>");
            System.exit(1);
        }

        Path inputPath = Path.of(args[0]);
        int numParts = Integer.parseInt(args[1]);

        if (numParts <= 0) {
            System.err.println("numParts must be > 0");
            System.exit(1);
        }

        // Prepare writers for output files
        BufferedWriter[] writers = new BufferedWriter[numParts];
        for (int i = 0; i < numParts; i++) {
            Path outPath = Path.of(inputPath + ".part" + (i + 1));
            writers[i] = Files.newBufferedWriter(outPath);
        }

        try (BufferedReader reader = Files.newBufferedReader(inputPath)) {
            String line;
            int fileIndex = 0;

            // Simple round-robin distribution
            while ((line = reader.readLine()) != null) {
                writers[fileIndex].write(line);
                writers[fileIndex].newLine();

                fileIndex = (fileIndex + 1) % numParts; // cycle to next writer
            }
        } finally {
            // Always close all writers
            for (BufferedWriter w : writers) {
                if (w != null) w.close();
            }
        }

        System.out.printf("Split complete: %d parts created from %s%n", numParts, inputPath);
    }
}
