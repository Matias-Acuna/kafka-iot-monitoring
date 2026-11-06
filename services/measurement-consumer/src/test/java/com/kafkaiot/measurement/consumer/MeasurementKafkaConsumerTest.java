package com.kafkaiot.measurement.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.model.SensorAlert;
import com.kafkaiot.measurement.model.SensorResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementKafkaConsumerTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final SensorResult RESULT = new SensorResult("event-1", "run-1", "device-1",
            1_700_000_000_000L, 1_700_000_000_100L, "RAPID_CHANGE", true);

    @TempDir
    Path tempDir;

    @Test
    void retainsExistingPublicConstructorAndArgumentValidationWithoutCreatingKafkaClient() throws Exception {
        assertTrue(MeasurementKafkaConsumer.class.getConstructor(Properties.class, MeasurementAggregator.class) != null);
        assertThrows(NullPointerException.class,
                () -> new MeasurementKafkaConsumer((Properties) null, new MeasurementAggregator()));
        assertThrows(NullPointerException.class,
                () -> new MeasurementKafkaConsumer(new Properties(), null));
    }

    @Test
    void absentWriterPreservesResultDuplicateAndAlertAggregation() throws Exception {
        var kafka = new TrackingConsumer();
        var aggregator = new MeasurementAggregator();
        var consumer = new MeasurementKafkaConsumer(kafka, aggregator, null);
        runRecords(consumer, kafka, resultRecord(0, RESULT), resultRecord(1, RESULT), alertRecord());

        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getUniqueResultCount());
        assertEquals(1, aggregator.getDuplicateResultCount());
        assertEquals(1, aggregator.getReceivedAlertCount());
        assertSame(Thread.currentThread(), kafka.closeThread);
        assertTrue(kafka.closed());
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void exportsUniqueAndDuplicateDeliveriesWithExactRegisteredValues() throws Exception {
        Path output = tempDir.resolve("results.csv");
        var kafka = new TrackingConsumer();
        var aggregator = new MeasurementAggregator();
        try (var writer = new SensorResultCsvWriter(output)) {
            var consumer = new MeasurementKafkaConsumer(kafka, aggregator, writer);
            runRecords(consumer, kafka, resultRecord(0, RESULT), resultRecord(1, RESULT));
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            for (int i = 0; i < 2; i++) {
                var received = aggregator.getReceivedResults().get(i);
                assertEquals(RESULT, received.result());
                assertEquals(i == 1, received.duplicate());
                assertEquals("run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                        + received.resultReceivedAt() + "," + received.latencyMs() + ","
                        + received.duplicate() + "," + (received.latencyMs() < 0), rows.get(i + 1));
            }
            assertEquals(2, aggregator.getReceivedResultCount());
            assertEquals(1, aggregator.getDuplicateResultCount());
        }
    }

    @Test
    void preservesNegativeLatencyWithoutNewClockReadings() throws Exception {
        Path output = tempDir.resolve("negative.csv");
        var kafka = new TrackingConsumer();
        var aggregator = new MeasurementAggregator();
        var futureResult = new SensorResult("event-1", "run-1", "device-1", RESULT.eventTime(),
                Long.MAX_VALUE, "RAPID_CHANGE", false);
        try (var writer = new SensorResultCsvWriter(output)) {
            var consumer = new MeasurementKafkaConsumer(kafka, aggregator, writer);
            runRecords(consumer, kafka, resultRecord(0, futureResult));
            writer.flush();
            var received = aggregator.getReceivedResults().getFirst();
            assertTrue(received.latencyMs() < 0);
            assertEquals(received.resultReceivedAt() - Long.MAX_VALUE, received.latencyMs());
            String[] fields = Files.readAllLines(output).get(1).split(",", -1);
            assertEquals(Long.toString(received.resultReceivedAt()), fields[7]);
            assertEquals(Long.toString(received.latencyMs()), fields[8]);
            assertEquals("true", fields[10]);
        }
    }

    @Test
    void alertsAreNotExportedAndConsumerLeavesWriterOpen() throws Exception {
        Path output = tempDir.resolve("alerts.csv");
        var kafka = new TrackingConsumer();
        var aggregator = new MeasurementAggregator();
        try (var writer = new SensorResultCsvWriter(output)) {
            var consumer = new MeasurementKafkaConsumer(kafka, aggregator, writer);
            runRecords(consumer, kafka, alertRecord());
            writer.flush();
            assertEquals(1, Files.readAllLines(output).size());
            assertEquals(1, aggregator.getReceivedAlertCount());
            assertEquals(0, aggregator.getReceivedResultCount());
            // A caller can continue using the writer after the Kafka consumer is closed.
            writer.writeObservation(aggregator.registerResult(RESULT, RESULT.sentAt() + 25));
            writer.flush();
            assertEquals(2, Files.readAllLines(output).size());
        }
    }

    @Test
    void csvFailureStopsProcessingAndPreservesCleanupFailure() throws Exception {
        Path output = tempDir.resolve("io-failure.csv");
        var writer = new SensorResultCsvWriter(output);
        writer.close();
        var kafka = new TrackingConsumer();
        var closeFailure = new IllegalStateException("mock Kafka close failed");
        kafka.closeFailure = closeFailure;
        var aggregator = new MeasurementAggregator();
        var consumer = new MeasurementKafkaConsumer(kafka, aggregator, writer);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> runRecords(consumer, kafka, resultRecord(0, RESULT), resultRecord(1, RESULT)));

        assertTrue(failure.getMessage().contains("Failed to write result CSV"));
        assertTrue(failure.getMessage().contains("topic sensor-results, partition 0, offset 0"));
        assertInstanceOf(IOException.class, failure.getCause());
        assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
        assertEquals(1, aggregator.getReceivedResultCount());
        assertTrue(kafka.closed());
    }

    @Test
    void malformedJsonKeepsOriginalCauseAndDoesNotCloseWriter() throws Exception {
        Path output = tempDir.resolve("malformed.csv");
        var kafka = new TrackingConsumer();
        var closeFailure = new IllegalStateException("mock close failed");
        kafka.closeFailure = closeFailure;
        var aggregator = new MeasurementAggregator();
        try (var writer = new SensorResultCsvWriter(output)) {
            var consumer = new MeasurementKafkaConsumer(kafka, aggregator, writer);
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> runRecords(consumer, kafka,
                            new ConsumerRecord<>("sensor-results", 0, 12, "key", "{invalid")));
            assertInstanceOf(JsonProcessingException.class, failure.getCause());
            assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
            assertEquals(0, aggregator.getReceivedResultCount());
            writer.flush();
            assertEquals(1, Files.readAllLines(output).size());
        }
    }

    @Test
    void closeFailureAfterNormalShutdownIsNotIgnored() throws Exception {
        var kafka = new TrackingConsumer();
        var closeFailure = new IllegalStateException("mock close failed");
        kafka.closeFailure = closeFailure;
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null);
        consumer.shutdown();
        assertSame(closeFailure, assertThrows(IllegalStateException.class, consumer::run));
        assertSame(Thread.currentThread(), kafka.closeThread);
    }

    @Test
    void unexpectedTopicFailsWithoutResultRegistration() throws Exception {
        var kafka = new TrackingConsumer();
        var aggregator = new MeasurementAggregator();
        var consumer = new MeasurementKafkaConsumer(kafka, aggregator, null);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> runRecords(consumer, kafka, new ConsumerRecord<>("unexpected", 0, 0, "key", "{}")));
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals(0, aggregator.getReceivedResultCount());
        assertTrue(kafka.closed());
    }

    @SafeVarargs
    private static void runRecords(MeasurementKafkaConsumer consumer, TrackingConsumer kafka,
                                   ConsumerRecord<String, String>... records) {
        kafka.schedulePollTask(() -> {
            List<TopicPartition> partitions = Arrays.stream(records)
                    .map(record -> new TopicPartition(record.topic(), record.partition())).distinct().toList();
            kafka.updateBeginningOffsets(partitions.stream().collect(Collectors.toMap(partition -> partition, p -> 0L)));
            kafka.rebalance(partitions);
            for (var record : records) {
                kafka.addRecord(record);
            }
        });
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
    }

    private static ConsumerRecord<String, String> resultRecord(long offset, SensorResult result)
            throws JsonProcessingException {
        return new ConsumerRecord<>("sensor-results", 0, offset, "device-1", OBJECT_MAPPER.writeValueAsString(result));
    }

    private static ConsumerRecord<String, String> alertRecord() throws JsonProcessingException {
        var alert = new SensorAlert("alert-1", "run-1", "device-1", RESULT.eventTime(), RESULT.sentAt(), "RAPID_CHANGE");
        return new ConsumerRecord<>("sensor-alerts", 0, 0, "device-1", OBJECT_MAPPER.writeValueAsString(alert));
    }

    private static final class TrackingConsumer extends MockConsumer<String, String> {
        private RuntimeException closeFailure;
        private Thread closeThread;

        private TrackingConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public void close() {
            closeThread = Thread.currentThread();
            super.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
