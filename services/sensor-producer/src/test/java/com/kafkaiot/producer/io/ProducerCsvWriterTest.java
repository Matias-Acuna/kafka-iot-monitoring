package com.kafkaiot.producer.io;

import com.kafkaiot.producer.metrics.ProducerMetrics;
import com.kafkaiot.producer.metrics.ProducerMetrics.PhaseSnapshot;
import com.kafkaiot.producer.metrics.ProducerMetrics.Snapshot;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerCsvWriterTest {
    private static final String HEADER = "runId,phase,attempted,confirmed,failed,latestError\r\n";

    @TempDir
    Path tempDir;

    @Test
    void writesExactlyOneHeaderAndTwoOrderedPhaseRows() throws IOException {
        Path output = tempDir.resolve("structure.csv");
        ProducerCsvWriter.write(output, new ProducerMetrics("structure").snapshot());

        assertEquals(HEADER
                + "structure,WARM_UP,0,0,0,\r\n"
                + "structure,MEASUREMENT,0,0,0,\r\n", read(output));
    }

    @Test
    void writesTheSuppliedSnapshotValues() throws IOException {
        Path output = tempDir.resolve("values.csv");
        Snapshot snapshot = new Snapshot("values",
                new PhaseSnapshot(Phase.WARM_UP, 10, 7, 2, "warm-up error"),
                new PhaseSnapshot(Phase.MEASUREMENT, 30, 25, 4, "measurement error"));
        ProducerCsvWriter.write(output, snapshot);

        assertEquals(HEADER
                + "values,WARM_UP,10,7,2,warm-up error\r\n"
                + "values,MEASUREMENT,30,25,4,measurement error\r\n", read(output));
    }

    @Test
    void writesNullAndEmptyErrorsAsEmptyFields() throws IOException {
        Path output = tempDir.resolve("empty-errors.csv");
        Snapshot snapshot = new Snapshot("empty",
                new PhaseSnapshot(Phase.WARM_UP, 1, 0, 1, null),
                new PhaseSnapshot(Phase.MEASUREMENT, 2, 1, 1, ""));
        ProducerCsvWriter.write(output, snapshot);

        assertEquals(HEADER
                + "empty,WARM_UP,1,0,1,\r\n"
                + "empty,MEASUREMENT,2,1,1,\r\n", read(output));
    }

    @Test
    void quotesEachCsvSpecialCharacterInRunIdsAndErrors() throws IOException {
        String[][] cases = {
                {"a,b", "\"a,b\""},
                {"a\"b", "\"a\"\"b\""},
                {"a\rb", "\"a\rb\""},
                {"a\nb", "\"a\nb\""}
        };
        Path output = tempDir.resolve("escaping.csv");
        for (String[] example : cases) {
            Snapshot snapshot = new Snapshot(example[0],
                    new PhaseSnapshot(Phase.WARM_UP, 1, 0, 1, example[0]),
                    new PhaseSnapshot(Phase.MEASUREMENT, 2, 1, 1, example[0]));
            ProducerCsvWriter.write(output, snapshot);

            String escaped = example[1];
            assertEquals(HEADER
                    + escaped + ",WARM_UP,1,0,1," + escaped + "\r\n"
                    + escaped + ",MEASUREMENT,2,1,1," + escaped + "\r\n", read(output));
        }
    }

    @Test
    void preservesUtf8AndUsesCrLfRowEndingsWithEmbeddedNewlines() throws IOException {
        Path output = tempDir.resolve("unicode.csv");
        Snapshot snapshot = new Snapshot("München-温度",
                new PhaseSnapshot(Phase.WARM_UP, 1, 0, 1, "échec\r\n次行"),
                new PhaseSnapshot(Phase.MEASUREMENT, 0, 0, 0, null));
        ProducerCsvWriter.write(output, snapshot);

        String expected = HEADER
                + "München-温度,WARM_UP,1,0,1,\"échec\r\n次行\"\r\n"
                + "München-温度,MEASUREMENT,0,0,0,\r\n";
        // Compare complete content rather than splitting quoted multiline fields.
        assertEquals(expected, read(output));
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), Files.readAllBytes(output));
    }

    @Test
    void createsMissingParentsAndOverwritesExistingOutput() throws IOException {
        Path output = tempDir.resolve("missing/nested/results.csv");
        ProducerCsvWriter.write(output, new ProducerMetrics("original-long-run-id").snapshot());
        assertTrue(Files.isDirectory(output.getParent()));

        ProducerCsvWriter.write(output, new ProducerMetrics("new").snapshot());
        assertEquals(HEADER
                + "new,WARM_UP,0,0,0,\r\n"
                + "new,MEASUREMENT,0,0,0,\r\n", read(output));
    }

    @Test
    void rejectsNullOutputPathAndNullSnapshot() {
        Snapshot snapshot = new ProducerMetrics("validation").snapshot();
        assertThrows(NullPointerException.class, () -> ProducerCsvWriter.write(null, snapshot));
        assertThrows(NullPointerException.class,
                () -> ProducerCsvWriter.write(tempDir.resolve("null.csv"), null));
    }

    private static String read(Path output) throws IOException {
        return Files.readString(output, StandardCharsets.UTF_8);
    }
}
