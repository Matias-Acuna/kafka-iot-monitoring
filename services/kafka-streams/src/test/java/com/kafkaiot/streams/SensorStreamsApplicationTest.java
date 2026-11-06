package com.kafkaiot.streams;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void acceptsHighTemperatureOptionsWithoutStartingKafkaStreams() throws Exception {
        assertEquals(Map.of("bootstrap-servers", "unused:9092", "application-id", "cli-test",
                "rule", "HIGH_TEMPERATURE", "temperature-threshold", "30.0"),
                parseWithoutStarting(baseArguments()));
    }

    @Test
    void acceptsHighFrequencyOptionsWithoutStartingKafkaStreams() throws Exception {
        assertEquals(Map.of("bootstrap-servers", "unused:9092", "application-id", "frequency-cli-test",
                "rule", "HIGH_FREQUENCY", "window-duration-ms", "1000", "count-threshold", "2"),
                parseWithoutStarting(highFrequencyArguments()));
    }

    @Test
    void rejectsEachMissingHighFrequencyParameter() {
        for (int optionIndex : new int[]{6, 8}) {
            List<String> arguments = new ArrayList<>(Arrays.asList(highFrequencyArguments()));
            arguments.subList(optionIndex, optionIndex + 2).clear();
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void highTemperatureRejectsFrequencyParametersIndividuallyAndTogether() {
        for (List<String> extra : List.of(List.of("--window-duration-ms", "1000"),
                List.of("--count-threshold", "2"),
                List.of("--window-duration-ms", "1000", "--count-threshold", "2"))) {
            List<String> arguments = new ArrayList<>(Arrays.asList(baseArguments()));
            arguments.addAll(extra);
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void highFrequencyRejectsTemperatureParameter() {
        List<String> arguments = new ArrayList<>(Arrays.asList(highFrequencyArguments()));
        arguments.addAll(List.of("--temperature-threshold", "30.0"));
        assertRejected(arguments.toArray(String[]::new));
    }

    @Test
    void rejectsInvalidHighFrequencyWindowDurations() {
        for (String value : new String[]{"0", "-1", "not-a-number", "1.5", "NaN", "Infinity",
                "9223372036854775808", "-9223372036854775809"}) {
            String[] arguments = highFrequencyArguments();
            arguments[7] = value;
            assertRejected(arguments);
        }
    }

    @Test
    void rejectsInvalidHighFrequencyCountThresholds() {
        for (String value : new String[]{"0", "-1", "not-a-number", "1.5", "NaN", "Infinity",
                "2147483648", "-2147483649"}) {
            String[] arguments = highFrequencyArguments();
            arguments[9] = value;
            assertRejected(arguments);
        }
    }

    @Test
    void highFrequencyRejectsEachMissingCommonOption() {
        for (int optionIndex : new int[]{0, 2, 4}) {
            List<String> arguments = new ArrayList<>(Arrays.asList(highFrequencyArguments()));
            arguments.subList(optionIndex, optionIndex + 2).clear();
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void highFrequencyRejectsUnknownOptions() {
        List<String> arguments = new ArrayList<>(Arrays.asList(highFrequencyArguments()));
        arguments.addAll(List.of("--unknown", "value"));
        assertRejected(arguments.toArray(String[]::new));
    }

    @Test
    void highFrequencyRejectsEachDuplicateOption() {
        String[] base = highFrequencyArguments();
        for (int i = 0; i < base.length; i += 2) {
            List<String> arguments = new ArrayList<>(Arrays.asList(base));
            arguments.addAll(List.of(base[i], base[i + 1]));
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void highFrequencyRejectsMissingEmptyAndBlankValues() {
        String[] base = highFrequencyArguments();
        for (int i = 1; i < base.length; i += 2) {
            for (String value : new String[]{"", " ", "\t\r\n"}) {
                String[] arguments = base.clone();
                arguments[i] = value;
                assertRejected(arguments);
            }
            List<String> arguments = new ArrayList<>(Arrays.asList(base));
            arguments.remove(i);
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rejectsUnsupportedRulesWithEitherParameterSet() {
        for (String rule : new String[]{"RAPID_CHANGE", "UNKNOWN", "high_frequency"}) {
            for (String[] arguments : List.of(baseArguments(), highFrequencyArguments())) {
                arguments[5] = rule;
                assertRejected(arguments);
            }
        }
    }

    @Test
    void reflectionExposesUnderlyingValidationException() {
        List<String> arguments = new ArrayList<>(Arrays.asList(highFrequencyArguments()));
        arguments.subList(6, 8).clear();

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> parseWithoutStarting(arguments.toArray(String[]::new)));

        assertTrue(failure.getMessage().contains("Missing required option --window-duration-ms"));
    }

    @Test
    void helpDocumentsBothRulesAndTheirRequiredParametersWithoutStartingStreams() {
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
        assertTrue(help.contains("--rule HIGH_TEMPERATURE|HIGH_FREQUENCY"));
        int temperatureSection = help.indexOf("Required for HIGH_TEMPERATURE:");
        int frequencySection = help.indexOf("Required for HIGH_FREQUENCY:");
        assertTrue(temperatureSection >= 0 && frequencySection > temperatureSection);
        assertTrue(help.substring(temperatureSection, frequencySection).contains("--temperature-threshold"));
        assertTrue(help.substring(frequencySection).contains("--window-duration-ms"));
        assertTrue(help.substring(frequencySection).contains("--count-threshold"));
        assertTrue(help.contains("--bootstrap-servers"));
        assertTrue(help.contains("--application-id"));
        assertTrue(errors.toString(StandardCharsets.UTF_8).isEmpty());
    }

    @Test
    void acceptsRapidChangeOptionsWithoutStartingKafkaStreams() throws Exception {
        assertEquals(Map.of("bootstrap-servers", "unused:9092", "application-id", "rapid-cli-test",
                "rule", "RAPID_CHANGE", "max-time-difference-ms", "1000",
                "temperature-change-threshold", "5.0"), parseWithoutStarting(rapidChangeArguments()));
    }

    @Test
    void rapidChangeRejectsEachMissingRuleParameter() {
        for (int index : new int[]{6, 8}) {
            List<String> arguments = new ArrayList<>(Arrays.asList(rapidChangeArguments()));
            arguments.subList(index, index + 2).clear();
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rapidChangeRejectsInvalidTimeLimits() {
        for (String value : new String[]{"0", "-1", "not-a-number", "1.5",
                "9223372036854775808", "-9223372036854775809"}) {
            String[] arguments = rapidChangeArguments();
            arguments[7] = value;
            assertRejected(arguments);
        }
    }

    @Test
    void rapidChangeRejectsInvalidTemperatureChangeThresholds() {
        for (String value : new String[]{"0", "-0.0", "-1", "NaN", "Infinity", "-Infinity",
                "not-a-number", "1e309", "1e-400"}) {
            String[] arguments = rapidChangeArguments();
            arguments[9] = value;
            assertRejected(arguments);
        }
    }

    @Test
    void rapidChangeRejectsParametersFromOtherRules() {
        for (List<String> extra : List.of(List.of("--temperature-threshold", "30.0"),
                List.of("--window-duration-ms", "1000"), List.of("--count-threshold", "2"),
                List.of("--window-duration-ms", "1000", "--count-threshold", "2"))) {
            List<String> arguments = new ArrayList<>(Arrays.asList(rapidChangeArguments()));
            arguments.addAll(extra);
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void existingRulesRejectRapidChangeParameters() {
        for (String[] base : List.of(baseArguments(), highFrequencyArguments())) {
            for (List<String> extra : List.of(List.of("--max-time-difference-ms", "1000"),
                    List.of("--temperature-change-threshold", "5.0"))) {
                List<String> arguments = new ArrayList<>(Arrays.asList(base));
                arguments.addAll(extra);
                assertRejected(arguments.toArray(String[]::new));
            }
        }
    }

    @Test
    void rapidChangeRejectsUnknownAndDuplicateOptions() {
        List<String> unknown = new ArrayList<>(Arrays.asList(rapidChangeArguments()));
        unknown.addAll(List.of("--unknown", "value"));
        assertRejected(unknown.toArray(String[]::new));
        String[] base = rapidChangeArguments();
        for (int i = 0; i < base.length; i += 2) {
            List<String> arguments = new ArrayList<>(Arrays.asList(base));
            arguments.addAll(List.of(base[i], base[i + 1]));
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rapidChangeRejectsMissingCommonOptions() {
        for (int index : new int[]{0, 2, 4}) {
            List<String> arguments = new ArrayList<>(Arrays.asList(rapidChangeArguments()));
            arguments.subList(index, index + 2).clear();
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void rapidChangeRejectsUnsupportedRuleNames() {
        for (String rule : new String[]{"UNKNOWN", "rapid_change", "RAPID_CHANGE_OTHER"}) {
            String[] arguments = rapidChangeArguments();
            arguments[5] = rule;
            assertRejected(arguments);
        }
    }

    @Test
    void rapidChangeRejectsMissingEmptyAndBlankValues() {
        String[] base = rapidChangeArguments();
        for (int i = 1; i < base.length; i += 2) {
            for (String value : new String[]{"", " ", "\t\r\n"}) {
                String[] arguments = base.clone();
                arguments[i] = value;
                assertRejected(arguments);
            }
            List<String> arguments = new ArrayList<>(Arrays.asList(base));
            arguments.remove(i);
            assertRejected(arguments.toArray(String[]::new));
        }
    }

    @Test
    void helpDocumentsRapidChangeAndItsParametersWithoutStartingStreams() {
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
        assertTrue(help.contains("--rule HIGH_TEMPERATURE|HIGH_FREQUENCY|RAPID_CHANGE"));
        int section = help.indexOf("Required for RAPID_CHANGE:");
        assertTrue(section >= 0);
        assertTrue(help.substring(section).contains("--max-time-difference-ms LONG"));
        assertTrue(help.substring(section).contains("--temperature-change-threshold DOUBLE"));
        assertTrue(errors.toString(StandardCharsets.UTF_8).isEmpty());
    }

    private static String[] rapidChangeArguments() {
        return new String[]{"--bootstrap-servers", "unused:9092", "--application-id", "rapid-cli-test",
                "--rule", "RAPID_CHANGE", "--max-time-difference-ms", "1000",
                "--temperature-change-threshold", "5.0"};
    }

    private static String[] highFrequencyArguments() {
        return new String[]{"--bootstrap-servers", "unused:9092", "--application-id", "frequency-cli-test",
                "--rule", "HIGH_FREQUENCY", "--window-duration-ms", "1000", "--count-threshold", "2"};
    }

    // Only validates option structure; numeric validation remains in main().
    private static Map<?, ?> parseWithoutStarting(String[] arguments) throws Exception {
        Method parse = SensorStreamsApplication.class.getDeclaredMethod("parse", String[].class);
        parse.setAccessible(true);
        try {
            return (Map<?, ?>) parse.invoke(null, (Object) arguments);
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception underlying) {
                throw underlying;
            }
            if (cause instanceof Error underlying) {
                throw underlying;
            }
            throw new AssertionError("Unexpected parser failure", cause);
        }
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
