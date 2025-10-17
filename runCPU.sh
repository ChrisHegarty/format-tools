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
java -cp "libs/*:libs" org.chegar.BulkJSONLoadGenerator \
 http://localhost:9200 vecs 500 8 /home/ubuntu/.rally/benchmarks/data/openai-initial-indexing/open_ai_corpus-initial-indexing_emb_only.json

# Gets the number of segments
curl -s "http://localhost:9200/vecs/_segments" | jq | grep segments

# Force merge to 3 segments
date
time curl -X POST "http://localhost:9200/vecs/_forcemerge?max_num_segments=3" \
    -H 'Content-Type: application/json'
date

curl -s "http://localhost:9200/vecs/_segments" | jq | grep segments