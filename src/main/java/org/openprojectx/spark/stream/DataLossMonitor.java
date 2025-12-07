package org.openprojectx.spark.stream;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DataLossMonitor extends StreamingQueryListener {

    private static final Logger log = LoggerFactory.getLogger(DataLossMonitor.class);

    private final AdminClient admin;

    public DataLossMonitor(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        StreamingQueryProgress progress = event.progress();

        Arrays.stream(progress.sources()).forEach(source -> {
            // Identify Kafka source
            if (!source.description().contains("KafkaV2")) {
                return;
            }

            String startOffsetJson = source.startOffset();
            String endOffsetJson = source.endOffset();

            Map<TopicPartition, Long> startOffsets = parseOffsetJson(startOffsetJson);
            Map<TopicPartition, Long> endOffsets = parseOffsetJson(endOffsetJson);

            detectDataLoss(startOffsets, endOffsets);
        });
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        log.info("Streaming query started. id=" + event.id());
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        log.info("Streaming query terminated. id=" + event.id());
    }

    private Map<TopicPartition, Long> parseOffsetJson(String json) {
        /*
         Example Spark JSON formats:

         {"orders":{"0":100,"1":60,"2":66}}
         {"topicA":{"0":130},"topicB":{"0":55}}
        */

        Map<TopicPartition, Long> result = new HashMap<>();

        if (json == null || json.isEmpty()) return result;

        String cleaned = json.trim();

        // Remove outer { }
        if (cleaned.startsWith("{")) cleaned = cleaned.substring(1);
        if (cleaned.endsWith("}")) cleaned = cleaned.substring(0, cleaned.length()-1);

        // Split per-topic-section: e.g. orders:{0:100,1:60}
        for (String topicSection : cleaned.split("},")) {
            topicSection = topicSection.replace("}", "");

            String[] parts = topicSection.split(":\\{");
            if (parts.length != 2) continue;

            String topic = parts[0].replace("\"", "").trim();
            String offsetList = parts[1];

            for (String kv : offsetList.split(",")) {
                String[] arr = kv.split(":");
                if (arr.length != 2) continue;

                int partition = Integer.parseInt(arr[0].replace("\"", "").trim());
                long offset = Long.parseLong(arr[1].trim());

                result.put(new TopicPartition(topic, partition), offset);
            }
        }

        return result;
    }

    private void detectDataLoss(Map<TopicPartition, Long> startOffsets,
                                Map<TopicPartition, Long> endOffsets) {

        for (TopicPartition tp : startOffsets.keySet()) {
            long expectedStart = startOffsets.get(tp);

            long earliestAvailable = earliestOffset(tp);
            long latestAvailable = latestOffset(tp);

            if (earliestAvailable > expectedStart) {
                long lostBegin = expectedStart;
                long lostEnd = earliestAvailable - 1;
                long lostCount = earliestAvailable - expectedStart;

                log.error(
                        "DATA LOSS DETECTED for {} partition {}. " +
                                "Expected to read from offset >= {}, but earliest available is {}. " +
                                "Lost range: {} → {} ({} messages)",
                        tp.topic(), tp.partition(),
                        expectedStart, earliestAvailable,
                        lostBegin, lostEnd, lostCount
                );
            }

            log.debug(
                    "Partition {} earliest={}, latest={}",
                    tp, earliestAvailable, latestAvailable
            );
        }
    }

    private long earliestOffset(TopicPartition tp) {
        try {
            ListOffsetsResult result =
                    admin.listOffsets(Collections.singletonMap(tp, OffsetSpec.earliest()));
            return result.partitionResult(tp).get().offset();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error fetching earliest offset for " + tp, e);
            return -1;
        }
    }

    private long latestOffset(TopicPartition tp) {
        try {
            ListOffsetsResult result =
                    admin.listOffsets(Collections.singletonMap(tp, OffsetSpec.latest()));
            return result.partitionResult(tp).get().offset();
        } catch (Exception e) {
            log.error("Error fetching latest offset for " + tp, e);
            return -1;
        }
    }
}
