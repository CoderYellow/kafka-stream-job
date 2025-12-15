package org.openprojectx.spark.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Kafka data-loss monitor for Spark BATCH jobs.
 *
 * Detects missing Kafka offsets BEFORE Spark reads data.
 * Does NOT affect Spark execution unless the caller chooses to.
 */
public class KafkaBatchDataLossMonitor {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchDataLossMonitor.class);

    private final AdminClient admin;
    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean ready;

    public KafkaBatchDataLossMonitor(String bootstrapServers) {
        AdminClient tmp = null;
        boolean ok = false;

        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            tmp = AdminClient.create(props);

            // Smoke test
            tmp.describeCluster().nodes().get();
            ok = true;
            log.info("KafkaBatchDataLossMonitor connected to Kafka at {}", bootstrapServers);

        } catch (Exception e) {
            log.error("KafkaBatchDataLossMonitor initialization failed. " +
                    "Data-loss detection disabled.", e);
        }

        this.admin = tmp;
        this.ready = ok;
    }

    /**
     * Check data loss for a batch Kafka read.
     *
     * @param startingOffsets JSON string used by Spark
     * @param endingOffsets   JSON string used by Spark
     * @return list of detected data-loss events (empty if none)
     */
    public List<DataLossEvent> check(String startingOffsets, String endingOffsets) {
        List<DataLossEvent> events = new ArrayList<>();

        if (!ready) {
            log.warn("KafkaBatchDataLossMonitor not ready; skipping check.");
            return events;
        }

        try {
            Map<TopicPartition, Long> start = parseOffsets(startingOffsets);
            Map<TopicPartition, Long> end   = parseOffsets(endingOffsets);

            for (TopicPartition tp : start.keySet()) {
                long expectedStart = start.get(tp);
                long expectedEnd   = end.getOrDefault(tp, expectedStart);

                long earliest = getEarliestOffset(tp);
                long latest   = getLatestOffset(tp);

                if (earliest == -1) continue;

                if (earliest > expectedStart) {
                    events.add(new DataLossEvent(
                            tp.topic(),
                            tp.partition(),
                            expectedStart,
                            earliest - 1,
                            earliest - expectedStart,
                            expectedEnd,
                            earliest,
                            latest
                    ));
                }
            }
        } catch (Exception e) {
            log.error("KafkaBatchDataLossMonitor error (ignored).", e);
        }

        return events;
    }

    // ----------------------------------------------------------------------

    private Map<TopicPartition, Long> parseOffsets(String json) throws Exception {
        Map<TopicPartition, Long> result = new HashMap<>();
        JsonNode root = mapper.readTree(json);

        for (Iterator<Map.Entry<String, JsonNode>> t = root.fields(); t.hasNext();) {
            Map.Entry<String, JsonNode> topic = t.next();
            for (Iterator<Map.Entry<String, JsonNode>> p = topic.getValue().fields(); p.hasNext();) {
                Map.Entry<String, JsonNode> part = p.next();
                result.put(
                        new TopicPartition(topic.getKey(), Integer.parseInt(part.getKey())),
                        part.getValue().asLong()
                );
            }
        }
        return result;
    }

    private long getEarliestOffset(TopicPartition tp) {
        try {
            return admin.listOffsets(
                    Map.of(tp, OffsetSpec.earliest())
            ).partitionResult(tp).get().offset();
        } catch (Exception e) {
            log.error("Failed to fetch earliest offset for {}", tp, e);
            return -1;
        }
    }

    private long getLatestOffset(TopicPartition tp) {
        try {
            return admin.listOffsets(
                    Map.of(tp, OffsetSpec.latest())
            ).partitionResult(tp).get().offset();
        } catch (Exception e) {
            log.error("Failed to fetch latest offset for {}", tp, e);
            return -1;
        }
    }

    // ----------------------------------------------------------------------

    public record DataLossEvent(
            String topic,
            int partition,
            long lostFrom,
            long lostTo,
            long lostCount,
            long expectedEnd,
            long kafkaEarliest,
            long kafkaLatest
    ) {}
}

