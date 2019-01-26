# The monitoring of Wikipedia editting status by Flink

```
flink run -m 127.0.0.1:6123 -c com.skt.skon.wikiedits.WikipediaAnalysis build/libs/wiki-edits-0.1.jar \
    --channel-list en,ko \
    --session-gap 5 \
    --output-brokers 127.0.0.1:9092 \
    --topic-summary wiki-edits-summary --topic-contents wiki-edits-contents \
    --checkpoint-data-uri hdfs://127.0.0.1:9000/flink \
    --checkpoint-state-backend fs \
    --checkpoint-interval $[1*60*60*1000]
```
