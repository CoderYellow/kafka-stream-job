package org.openprojectx.spark.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class CheckpointKafkaDataLossMonitor {

    private static final Logger log =
            LoggerFactory.getLogger(CheckpointKafkaDataLossMonitor.class);

    private final Path checkpointPath;
    private final AdminClient admin;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointKafkaDataLossMonitor(
            String checkpointDir,
            String bootstrapServers
    ) throws Exception {

        this.checkpointPath = Paths.get(checkpointDir);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
    }

    public void checkLatestBatch() {
        try {
            Path offsetsDir = checkpointPath.resolve("offsets");
            List<Integer> batches = Files.list(offsetsDir)
                    .map(p -> p.getFileName().toString())
                    .filter(s -> s.matches("\\d+"))
                    .map(Integer::parseInt)
                    .sorted()
                    .collect(Collectors.toList());

            if (batches.size() < 2) return;

            int prev = batches.get(batches.size() - 2);
            int curr = batches.get(batches.size() - 1);

            Map<TopicPartition, Long> prevOffsets =
                    readOffsets(offsetsDir.resolve(String.valueOf(prev)));
            Map<TopicPartition, Long> currOffsets =
                    readOffsets(offsetsDir.resolve(String.valueOf(curr)));

            for (TopicPartition tp : currOffsets.keySet()) {
                long prevEnd = prevOffsets.getOrDefault(tp, -1L);
                long currStart = currOffsets.get(tp);

                long earliest = earliestKafkaOffset(tp);

                if (prevEnd >= 0 &&
                        prevEnd < earliest &&
                        currStart == earliest) {

                    log.error(
                            "POSSIBLE DATA LOSS DETECTED via checkpoint: " +
                                    "{} partition {}. prevEnd={}, kafkaEarliest={}, currStart={}",
                            tp.topic(), tp.partition(),
                            prevEnd, earliest, currStart
                    );
                }
            }
        } catch (Exception e) {
            log.warn("CheckpointKafkaDataLossMonitor failed (ignored)", e);
        }
    }

    private Map<TopicPartition, Long> readOffsets(Path file) throws Exception {
        List<String> lines = Files.readAllLines(file);
        String json = lines.get(lines.size() - 1); // offsets JSON is last line
        JsonNode root = mapper.readTree(json);

        Map<TopicPartition, Long> result = new HashMap<>();
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
