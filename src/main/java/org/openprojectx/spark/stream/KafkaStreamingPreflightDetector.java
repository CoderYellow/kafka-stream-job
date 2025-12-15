package org.openprojectx.spark.stream;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Option A:
 * Pre-flight Kafka data-loss detector for Spark Structured Streaming.
 *
 * ✔ Detects only (never throws)
 * ✔ Human-readable diagnostics
 * ✔ Safe to run before job start
 * ✔ Does NOT affect Spark job startup
 */
public class KafkaStreamingPreflightDetector {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaStreamingPreflightDetector.class);

    private final Path checkpointDir;
    private final AdminClient admin;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaStreamingPreflightDetector(
            String checkpointLocation,
            String bootstrapServers
    ) {

        this.checkpointDir = Paths.get(checkpointLocation);

        AdminClient tmp = null;
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            tmp = AdminClient.create(props);

            // light connectivity check
            tmp.describeCluster().nodes().get();

        } catch (Exception e) {
            log.warn(
                    "Kafka preflight detector disabled: unable to connect to Kafka ({}). " +
                            "Streaming job will continue without preflight validation.",
                    bootstrapServers,
                    e
            );
        }
        this.admin = tmp;
    }

    /**
     * Perform detection and log results.
     * This method NEVER throws.
     */
    public void detect() {

        if (admin == null) {
            log.warn("Kafka preflight detector skipped (Kafka client not available).");
            return;
        }

        try {
            Map<TopicPartition, Long> checkpointOffsets =
                    readLastCheckpointOffsets();

            if (checkpointOffsets.isEmpty()) {
                log.info(
                        "Kafka preflight check: no previous checkpoint offsets found. " +
                                "This appears to be the first run of the streaming job."
                );
                return;
            }

            log.info("Kafka preflight check started at {}", Instant.now());

            boolean lossDetected = false;

            for (Map.Entry<TopicPartition, Long> e : checkpointOffsets.entrySet()) {

                TopicPartition tp = e.getKey();
                long lastProcessedOffset = e.getValue();
                long kafkaEarliest = earliestKafkaOffset(tp);

                if (kafkaEarliest > lastProcessedOffset) {
                    lossDetected = true;

                    long lostFrom = lastProcessedOffset;
                    long lostTo = kafkaEarliest - 1;
                    long lostCount = kafkaEarliest - lastProcessedOffset;

                    log.error(
                            """
                            ⚠️  KAFKA DATA LOSS DETECTED (pre-start check)
    
                            Topic            : {}
                            Partition        : {}
                            Last processed   : offset {}
                            Kafka earliest   : offset {}
                            Records lost     : {} (offsets {} → {})
    
                            What this means:
                              • Spark previously committed offset {}
                              • Kafka no longer retains data before offset {}
                              • If the job starts now, Spark WILL skip these records
    
                            Common causes:
                              • Job was stopped longer than Kafka retention
                              • Kafka retention settings were reduced
                              • Topic data was manually deleted or compacted
                              • Checkpoint was restored from an old backup
    
                            Recommended actions:
                              • Investigate why the job was down
                              • Consider increasing Kafka retention
                              • Decide whether skipping this data is acceptable
                              • If NOT acceptable, restore data or reset checkpoint
                            """,
                            tp.topic(),
                            tp.partition(),
                            lastProcessedOffset,
                            kafkaEarliest,
                            lostCount,
                            lostFrom,
                            lostTo,
                            lastProcessedOffset,
                            kafkaEarliest
                    );
                }
            }

            if (!lossDetected) {
                log.info(
                        "Kafka preflight check PASSED: checkpoint offsets are consistent with Kafka retention. " +
                                "No existing data loss detected."
                );
            }

        } catch (Exception e) {
            log.warn(
                    "Kafka preflight check failed due to unexpected error (ignored). " +
                            "Streaming job will continue.",
                    e
            );
        }
    }

    // ----------------------------------------------------------------------

    private Map<TopicPartition, Long> readLastCheckpointOffsets() throws Exception {

        Path offsetsDir = checkpointDir.resolve("offsets");

        if (!Files.exists(offsetsDir)) {
            return Collections.emptyMap();
        }

        List<Integer> batches = Files.list(offsetsDir)
                .map(p -> p.getFileName().toString())
                .filter(s -> s.matches("\\d+"))
                .map(Integer::parseInt)
                .sorted()
                .collect(Collectors.toList());

        if (batches.isEmpty()) {
            return Collections.emptyMap();
        }

        int lastBatch = batches.get(batches.size() - 1);
        Path offsetFile = offsetsDir.resolve(String.valueOf(lastBatch));

        List<String> lines = Files.readAllLines(offsetFile);
        String json = lines.get(lines.size() - 1); // offsets JSON line

        return parseOffsets(json);
    }

    private Map<TopicPartition, Long> parseOffsets(String json) throws Exception {
        Map<TopicPartition, Long> result = new HashMap<>();
        JsonNode root = mapper.readTree(json);

        root.fields().forEachRemaining(topic -> {
            topic.getValue().fields().forEachRemaining(part -> {
                result.put(
                        new TopicPartition(
                                topic.getKey(),
                                Integer.parseInt(part.getKey())),
                        part.getValue().asLong()
                );
            });
        });

        return result;
    }

    private long earliestKafkaOffset(TopicPartition tp) throws Exception {
        return admin.listOffsets(
                Map.of(tp, OffsetSpec.earliest())
        ).partitionResult(tp).get().offset();
    }
}
