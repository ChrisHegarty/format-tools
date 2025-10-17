
### Dependencies (mostly Jackson)

mkdir libs; cd libs
wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.18.0/jackson-core-2.18.0.jar
wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.18.0/jackson-databind-2.18.0.jar
wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.18.0/jackson-annotations-2.18.0.jar
wget https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-smile/2.18.0/jackson-dataformat-smile-2.18.0.jar
wget https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar

# Compile all the source

javac -cp "libs/*" -d libs `find app/src/main/java/ -name "*.java"`

# I use this to strip the other fields from the dataset, but could just ignore them in the mapping.

java -cp "libs/*:libs" org.chegar.OpenAIStripFields \
 ~/data/open_ai_corpus-parallel-indexing.json \
 ~/data/open_ai_corpus-parallel-indexing_emb_only.json

 # Or, using gradle
./gradlew :app:strip  \
 ~/data/open_ai_corpus-parallel-indexing.json \
 ~/data/open_ai_corpus-parallel-indexing_emb_only.json

---

# Start Elasticsearch in one terminal

./build/distribution/local/elasticsearch-9.2.0-SNAPSHOT/bin/elasticsearch -Expack.security.enabled=false

# In another terminal, create the index, index the vectors, then check the stats

# clean up from previous run
curl -X DELETE "http://localhost:9200/vecs" | jq

# create the index with the given mappings
curl -X PUT "http://localhost:9200/vecs" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.store.preload": [ "vec", "vex", "vem", "veq", "veqm", "veb", "vebm"],
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index.refresh_interval": -1,
    "index.vectors.indexing.use_gpu": false
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "emb": {
        "type": "dense_vector",
        "element_type": "float",
        "dims": 1536,
        "index": true,
        "similarity": "dot_product",
        "index_options": {
          "type": "hnsw"
        }
      }
    }
  }
}' | jq

# Index the data
Usage: java BulkJSONLoadGenerator <esUrl> <indexName> <bulkSize> <indexingThreads> <filePath>

java -cp "libs/*:libs" org.chegar.BulkJSONLoadGenerator \
 http://localhost:9200 vecs 500 8 ~/data/open_ai_corpus-initial-indexing_emb_only.json

# Or, using gradle
./gradlew :app:run http://localhost:9200 vecs 500 8 ~/data/open_ai_corpus-initial-indexing_emb_only.json

# Refresh to sure that the in-memory buffers are flushed
curl -X POST "http://localhost:9200/vecs/_refresh" | jq

# checkout the vector count
curl -X GET "http://localhost:9200/_cluster/stats?filter_path=indices.field_stats,indices.indexing,indices.docs,indices.mappings&pretty"

--------


OpenAI dataset

$ date; wc -l open_ai_corpus-initial-indexing.json; date
Thu Oct 16 09:56:49 UTC 2025
2580961 open_ai_corpus-initial-indexing.json
Thu Oct 16 10:12:58 UTC 2025


-rw-rw-r--  1 ubuntu ubuntu  85G Nov 14  2023 open_ai_corpus-initial-indexing.json
-rw-rw-r--  1 ubuntu ubuntu  79G Oct 16 10:19 open_ai_corpus-initial-indexing_emb_only.json


----
Enable logging:

curl -X PUT "http://localhost:9200/_cluster/settings"   -H "Content-Type: application/json"   -d '{
  "transient": {
    "logger.org.elasticsearch.xpack.gpu.codec.ES92GpuHnswVectorsWriter": "DEBUG"
  }
}' | jq

# Gets the number of segments
curl -s "http://localhost:9200/vecs/_segments" \
| jq '.indices.vecs.shards[][] | {shard: .shard, segments: .num_search_segments}'



# Force merge to 3 segments
curl -X POST "http://localhost:9200/vecs/_forcemerge?max_num_segments=3" \
     -H 'Content-Type: application/json'

----
# Test that the GPU is setup correctly

curl -X DELETE "http://localhost:9200/vecs" | jq

curl -X PUT "http://localhost:9200/vecs" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.store.preload": [ "vec", "vex", "vem", "veq", "veqm", "veb", "vebm"],
    "index.number_of_shards": 1,
    "index.number_of_replicas": 0,
    "index.vectors.indexing.use_gpu": true
  },
  "mappings": {
    "dynamic": false,
    "properties": {
      "emb": {
        "type": "dense_vector",
        "element_type": "float",
        "index": true,
        "index_options": {
          "type": "hnsw"
        }
      }
    }
  }
}' | jq

Try just three
curl -X POST "http://localhost:9200/vecs/_bulk?refresh=wait_for" -H "Content-Type: application/json" -d '
{ "index": {} }
{ "emb": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]}
{ "index": {} }
{ "emb": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]}
{ "index": {} }
{ "emb": [0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3]}
' | jq