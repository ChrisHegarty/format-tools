package org.chegar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

// Strips the other fields from the dataset, leaving only the vector embedding, emb
// Assumption, there is an emb field that contains the vector embedding.
public class OpenAIStripFields {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: java OpenAIStripFields <input.ndjson> <output.ndjson>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        ObjectMapper mapper = new ObjectMapper();
        long count = 0;

        try (InputStream fis = Files.newInputStream(Paths.get(inputPath));
             BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
             PrintWriter writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream(outputPath)))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue; // skip empty lines
                }

                JsonNode node = mapper.readTree(line);
                // Extract the "emb" field
                JsonNode embNode = Objects.requireNonNull(node.get("emb"));
                ObjectNode newNode = mapper.createObjectNode();
                newNode.set("emb", embNode);
                writer.println(mapper.writeValueAsString(newNode));
                count++;

                if (count % 10_000 == 0) {
                    System.out.println("lines processed " + count);
                }
            }
        }
        System.out.println("Conversion complete:" + count + ",  output written to " + outputPath);
    }
}
