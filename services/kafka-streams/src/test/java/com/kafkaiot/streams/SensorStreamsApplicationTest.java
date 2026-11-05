package com.kafkaiot.streams;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorStreamsApplicationTest {
    @Test
    void helpAloneDisplaysUsageAndReturns() {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (PrintStream capturedOut = new PrintStream(output, true, StandardCharsets.UTF_8);
             PrintStream capturedErr = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(capturedOut);
                System.setErr(capturedErr);
                assertDoesNotThrow(() -> SensorStreamsApplication.main(new String[]{"--help"}));
            } finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
        }

        String help = output.toString(StandardCharsets.UTF_8);
        assertTrue(help.contains("Usage:"));
        for (String option : new String[]{"--bootstrap-servers", "--application-id",
                "--rule", "--temperature-threshold"}) {
            assertTrue(help.contains(option));
        }
        assertTrue(errors.toString(StandardCharsets.UTF_8).isEmpty());
    }

    @Test
    void rejectsMissingRequiredOptions() {
        assertRejected();
        for (int i = 0; i < 8; i += 2) {
            List<String> arguments = new ArrayList<>(Arrays.asList(baseArguments()));
            arguments.subList(i, i + 2).clear();
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rejectsUnknownOptions() {
        List<String> arguments = new ArrayList<>(Arrays.asList(baseArguments()));
        arguments.addAll(List.of("--unknown", "value"));
        assertRejected(arguments.toArray(String[]::new));
    }

    @Test
    void rejectsDuplicateOptions() {
        String[] base = baseArguments();
        for (int i = 0; i < base.length; i += 2) {
            List<String> arguments = new ArrayList<>(Arrays.asList(base));
            arguments.addAll(List.of(base[i], base[i + 1]));
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rejectsMissingEmptyAndBlankValues() {
        String[] base = baseArguments();
        for (int i = 1; i < base.length; i += 2) {
            for (String value : new String[]{"", " ", "\t\r\n"}) {
                String[] arguments = base.clone();
                arguments[i] = value;
                assertRejected(arguments);
            }
            List<String> missingValue = new ArrayList<>(Arrays.asList(base));
            missingValue.remove(i);
            assertRejected(missingValue.toArray(String[]::new));
        }
    }

    @Test
    void rejectsUnsupportedRules() {
        for (String rule : new String[]{"HIGH_FREQUENCY", "RAPID_CHANGE", "UNKNOWN"}) {
            String[] arguments = baseArguments();
            arguments[5] = rule;
            assertRejected(arguments);
        }
    }

    @Test
    void rejectsNonnumericAndNonFiniteThresholds() {
        for (String threshold : new String[]{"not-a-number", "NaN", "Infinity", "-Infinity"}) {
            String[] arguments = baseArguments();
            arguments[7] = threshold;
            assertRejected(arguments);
        }
    }

    @Test
    void rejectsHelpCombinedWithOtherArguments() {
        List<String> arguments = new ArrayList<>(Arrays.asList(baseArguments()));
        arguments.addFirst("--help");
        assertRejected(arguments.toArray(String[]::new));
        arguments.removeFirst();
        arguments.add("--help");
        assertRejected(arguments.toArray(String[]::new));
        assertRejected("--help", "value");
    }

    // A fresh argument template; every invocation using it first makes it invalid.
    private static String[] baseArguments() {
        return new String[]{"--bootstrap-servers", "unused:9092",
                "--application-id", "cli-test", "--rule", "HIGH_TEMPERATURE",
                "--temperature-threshold", "30.0"};
    }

    private static void assertRejected(String... arguments) {
        PrintStream originalErr = System.err;
        try (PrintStream capturedErr = new PrintStream(new ByteArrayOutputStream(),
                true, StandardCharsets.UTF_8)) {
            try {
                System.setErr(capturedErr);
                assertThrows(IllegalArgumentException.class,
                        () -> SensorStreamsApplication.main(arguments));
            } finally {
                System.setErr(originalErr);
            }
        }
    }
}
