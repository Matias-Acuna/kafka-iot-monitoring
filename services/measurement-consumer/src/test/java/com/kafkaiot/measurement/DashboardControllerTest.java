package com.kafkaiot.measurement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.dashboard.DashboardController;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.model.SensorAlert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Local ephemeral HTTP only; never connects to Kafka or project service ports. */
@Timeout(10)
class DashboardControllerTest {
    private static HttpResponse<String> get(HttpClient client, int port, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void emptyEndpointHasTheFrontendContractAndUnavailableValues() throws Exception {
        try (var client = HttpClient.newHttpClient(); var dashboard = new DashboardController(0, new MeasurementAggregator())) {
            dashboard.start();
            var response = get(client, dashboard.port(), "/metrics/dashboard");
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));
            var json = new ObjectMapper().readTree(response.body());
            for (String field : new String[]{"runId", "observedRule", "totalResults", "uniqueResults", "duplicateResults",
                    "totalAlerts", "resultThroughputPerSecond", "latencyP50Ms", "latencyP95Ms", "latencyP99Ms", "lastUpdatedAt"}) {
                assertTrue(json.has(field), field);
                assertTrue(json.get(field).isNull(), field);
            }
            assertEquals(0, json.path("throughputSeries").size());
            assertEquals(0, json.path("latencySeries").size());
            assertTrue(json.path("alertsByRule").path("HIGH_TEMPERATURE").isNull());
        }
    }

    @Test
    void staticResourcesAreServedLocallyWithCorrectTypes() throws Exception {
        try (var client = HttpClient.newHttpClient(); var dashboard = new DashboardController(0, new MeasurementAggregator())) {
            dashboard.start();
            for (var entry : Map.of("/", "text/html", "/dashboard.css", "text/css", "/dashboard.js", "application/javascript",
                    "/vendor/chart.umd.min.js", "application/javascript").entrySet()) {
                var response = get(client, dashboard.port(), entry.getKey());
                assertEquals(200, response.statusCode(), entry.getKey());
                assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith(entry.getValue()));
                assertFalse(response.body().isEmpty());
            }
            assertTrue(get(client, dashboard.port(), "/").body().contains("vendor/chart.umd.min.js"));
            assertTrue(get(client, dashboard.port(), "/vendor/chart.umd.min.js").body().contains("Chart.js v4.5.1"));
        }
    }

    @Test
    void unknownPathsAndNonGetMethodsAreRejected() throws Exception {
        try (var client = HttpClient.newHttpClient(); var dashboard = new DashboardController(0, new MeasurementAggregator())) {
            dashboard.start();
            for (String path : new String[]{"/etc/passwd", "/metrics/dashboard/extra", "/static/../pom.xml"}) {
                assertEquals(404, get(client, dashboard.port(), path).statusCode());
            }
            var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + dashboard.port() + "/metrics/dashboard"))
                    .timeout(Duration.ofSeconds(3)).POST(HttpRequest.BodyPublishers.noBody()).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(405, response.statusCode());
            assertEquals("GET", response.headers().firstValue("Allow").orElseThrow());
        }
    }

    @Test
    void actualAlertCountersSerializeAsNumbersIncludingZeros() throws Exception {
        var aggregator = new MeasurementAggregator();
        try (var client = HttpClient.newHttpClient(); var dashboard = new DashboardController(0, aggregator)) {
            dashboard.start();
            aggregator.registerAlert(new SensorAlert("e", "r", "d", 1, 2, "HIGH_FREQUENCY"));
            var json = new ObjectMapper().readTree(get(client, dashboard.port(), "/metrics/dashboard").body());
            assertEquals(0, json.path("totalResults").longValue());
            assertEquals(1, json.path("totalAlerts").longValue());
            assertEquals(1, json.path("alertsByRule").path("HIGH_FREQUENCY").longValue());
            assertEquals(0, json.path("alertsByRule").path("HIGH_TEMPERATURE").longValue());
            assertEquals("r", json.path("runId").textValue());
        }
    }

    @Test
    void closeIsIdempotentAndStopsTheActualServer() throws Exception {
        var dashboard = new DashboardController(0, new MeasurementAggregator());
        int port = dashboard.port();
        try (var client = HttpClient.newHttpClient()) {
            dashboard.start();
            assertEquals(200, get(client, port, "/").statusCode());
            dashboard.close();
            dashboard.close();
            assertThrows(IOException.class, () -> get(client, port, "/"));
            assertThrows(IllegalStateException.class, dashboard::start);
        } finally {
            dashboard.close();
        }
    }

    @Test
    void invalidConstructorArgumentsFailWithoutOpeningAServer() {
        assertThrows(IllegalArgumentException.class, () -> new DashboardController(-1, new MeasurementAggregator()));
        assertThrows(IllegalArgumentException.class, () -> new DashboardController(65536, new MeasurementAggregator()));
        assertThrows(NullPointerException.class, () -> new DashboardController(0, null));
    }
    @Test
    void legacyConstructorKeepsTheIpv4LoopbackBinding() throws Exception {
        try (var dashboard = new DashboardController(0, new MeasurementAggregator())) {
            assertEquals("127.0.0.1", dashboard.bindAddress().getAddress().getHostAddress());
            assertTrue(dashboard.bindAddress().getAddress().isLoopbackAddress());
        }
    }

    @Test
    void explicitWildcardBindsAllIpv4InterfacesAndPreservesTheMetricsEndpoint() throws Exception {
        try (var client = HttpClient.newHttpClient();
             var dashboard = new DashboardController("0.0.0.0", 0, new MeasurementAggregator())) {
            assertTrue(dashboard.bindAddress().getAddress().isAnyLocalAddress());
            dashboard.start();
            // get() connects to literal IPv4 127.0.0.1, regardless of the wildcard's textual representation.
            var response = get(client, dashboard.port(), "/metrics/dashboard");
            assertEquals("127.0.0.1", response.uri().getHost());
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));
            var json = new ObjectMapper().readTree(response.body());
            assertTrue(json.isObject());
            assertTrue(json.has("totalResults"));
        }
    }

    @Test
    void hostnameAndIpSyntaxAreValidatedWithoutOpeningAListener() {
        for (String host : new String[]{"127.0.0.1", "0.0.0.0", "localhost", "::1"}) {
            assertEquals(host, DashboardController.validateHost(host));
        }
        for (String host : new String[]{"", " ", "host name", "http://localhost", "127.0.0.1:8080", "host/path", "host?x"}) {
            assertThrows(IllegalArgumentException.class, () -> DashboardController.validateHost(host), host);
        }
        assertThrows(IllegalArgumentException.class, () -> DashboardController.validateHost(null));
    }

}
