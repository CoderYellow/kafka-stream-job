package org.openprojectx.spark.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DataLossMonitor extends StreamingQueryListener {

    private static final Logger log = LoggerFactory.getLogger(DataLossMonitor.class);

    private final AdminClient admin;
    private final ObjectMapper mapper = new ObjectMapper();

    public DataLossMonitor(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        log.info("Streaming query started: id={}, runId={}", event.id(), event.runId());
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        StreamingQueryProgress progress = event.progress();

        for (SourceProgress source : progress.sources()) {
            // We only care about Kafka sources
            if (!source.description().contains("KafkaV2")) {
                continue;
            }

            String startOffsetJson = source.startOffset();  // String JSON
            String endOffsetJson = source.endOffset();    // String JSON

            if (startOffsetJson == null || startOffsetJson.isEmpty()
                    || "null".equals(startOffsetJson)) {
                // First batch may have empty startOffset; skip
                continue;
            }

            try {
                Map<TopicPartition, Long> startOffsets = parseOffsets(startOffsetJson);
                Map<TopicPartition, Long> endOffsets = parseOffsets(endOffsetJson);

                detectDataLoss(startOffsets, endOffsets);
            } catch (Exception e) {
                log.error("Failed to parse Kafka offsets from progress: start={}, end={}",
                        startOffsetJson, endOffsetJson, e);
            }
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        if (event.exception().isDefined()) {
            log.error("Streaming query terminated with exception: id={}",
                    event.id(), event.exception().get());
        } else {
            log.info("Streaming query terminated normally: id={}", event.id());
        }
    }

    /**
     * Parse Kafka offsets JSON produced by SourceProgress.startOffset/endOffset.
     * Example: {"orders":{"0":100,"1":60,"2":66}}
     */
    private Map<TopicPartition, Long> parseOffsets(String json) throws Exception {
        Map<TopicPartition, Long> result = new HashMap<>();
        if (json == null || json.isEmpty() || "null".equals(json)) {
            return result;
        }

        JsonNode root = mapper.readTree(json);
        // root fields: topic -> { partition -> offset }
        Iterator<Map.Entry<String, JsonNode>> topics = root.fields();
        while (topics.hasNext()) {
            Map.Entry<String, JsonNode> topicEntry = topics.next();
            String topic = topicEntry.getKey();
            JsonNode partitionsNode = topicEntry.getValue();

            Iterator<Map.Entry<String, JsonNode>> parts = partitionsNode.fields();
            while (parts.hasNext()) {
                Map.Entry<String, JsonNode> p = parts.next();
                int partition = Integer.parseInt(p.getKey());
                long offset = p.getValue().asLong();
                result.put(new TopicPartition(topic, partition), offset);
            }
        }
        return result;
    }

    /**
     * Detects data loss by comparing Spark's expected offsets with Kafka's earliest offsets.
     */
    private void detectDataLoss(Map<TopicPartition, Long> start, Map<TopicPartition, Long> end) {
        for (TopicPartition tp : start.keySet()) {
            long expectedStart = start.get(tp);
            long expectedEnd = end.getOrDefault(tp, expectedStart);

            long earliest = getEarliestOffset(tp);
            long latest = getLatestOffset(tp);

            if (earliest == -1L) {
                // Could not fetch offsets; log and continue
                log.warn("Could not fetch earliest offset for {}", tp);
                continue;
            }

            // Data loss if Kafka earliest offset is greater than what Spark thinks
            if (earliest > expectedStart) {
                long lostFrom = expectedStart;
                long lostTo = earliest - 1;
                long lostCount = earliest - expectedStart;

                log.error(
                        "DATA LOSS DETECTED for {}: expected to read from offset {} "
                                + "but earliest available is {}. Lost range: {} → {} ({} messages). "
                                + "Expected batch range (Spark view): {} → {}",
                        tp, expectedStart, earliest,
                        lostFrom, lostTo, lostCount,
                        expectedStart, expectedEnd
                );

                // TODO: here you can emit metrics, push to Prometheus, send alert, etc.
            } else {
                log.debug("No data loss for {}. expectedStart={}, earliest={}, latest={}",
                        tp, expectedStart, earliest, latest);
            }
        }
    }

    private long getEarliestOffset(TopicPartition tp) {
        try {
            ListOffsetsResult result = admin.listOffsets(
                    Collections.singletonMap(tp, OffsetSpec.earliest()));
            return result.partitionResult(tp).get().offset();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error fetching earliest offset for " + tp, e);
            return -1L;
        }
    }

    private long getLatestOffset(TopicPartition tp) {
        try {
            ListOffsetsResult result = admin.listOffsets(
                    Collections.singletonMap(tp, OffsetSpec.latest()));
            return result.partitionResult(tp).get().offset();
        } catch (Exception e) {
            log.error("Error fetching latest offset for " + tp, e);
            return -1L;
        }
    }
}
