package org.chegar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class NdjsonToSmile {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java NdjsonToSmile <input.ndjson> <output.bin>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        ObjectMapper jsonMapper = new ObjectMapper();
        ObjectMapper smileMapper = new ObjectMapper(new SmileFactory());
        long count = 0;

        try (InputStream fis = Files.newInputStream(Paths.get(inputPath));
             // LZ4FrameInputStream lz4In = new LZ4FrameInputStream(fis);
             BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputPath)))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue; // skip empty lines
                }

                JsonNode node = jsonMapper.readTree(line);
                byte[] smileBytes = smileMapper.writeValueAsBytes(node);

                out.writeInt(smileBytes.length);
                out.write(smileBytes);
                count++;

                if (count % 100_000 == 0) {
                    System.out.println("lines processed " + count);
                }
            }
        }
        System.out.println("Conversion complete:" + count + ",  output written to " + outputPath);
    }

}
