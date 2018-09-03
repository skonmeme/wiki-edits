# Wikipedia edit status monitoring with Flink

```
flink run -m 127.0.0.1:6123 -c com.skt.skon.wikiedits.WikipediaAnalysis target/wiki-edits-0.1.jar \
    --channel-list en,ko,jp,de,es,fr,ru,pt,it,zh,pl \
    --session-gap 60 \
    --brokers 127.0.0.1:9092 \
    --topic-summary wiki-edits-summary --topic-contents wiki-edits-contents \
    --group-id other \
    --checkpoint-data-uri hdfs://127.0.0.1:9000/flink \
    --checkpoint-state-backend fs \
    --checkpoint-interval $((5*1000*60))
```
