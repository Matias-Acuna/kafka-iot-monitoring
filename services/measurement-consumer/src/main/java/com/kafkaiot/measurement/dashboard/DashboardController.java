package com.kafkaiot.measurement.dashboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/** Optional in-process operational dashboard; never reads experimental result files. */
public final class DashboardController implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, String> RESOURCES = Map.of(
            "/", "index.html", "/dashboard.css", "dashboard.css", "/dashboard.js", "dashboard.js",
            "/vendor/chart.umd.min.js", "vendor/chart.umd.min.js");
    private final HttpServer server;
    private final MeasurementAggregator aggregator;
    private boolean started;
    private boolean closed;

    /** Port zero supports isolated local tests; the application CLI requires 1..65535. */
    public DashboardController(int port, MeasurementAggregator aggregator) throws IOException {
        if (port < 0 || port > 65535) throw new IllegalArgumentException("Invalid dashboard port");
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        try {
            aggregator.enableDashboard();
            server.createContext("/", this::handle);
        } catch (RuntimeException | Error failure) {
            server.stop(0);
            throw failure;
        }
    }

    public synchronized void start() {
        if (started || closed) throw new IllegalStateException("Dashboard cannot be started twice or after closure");
        server.start(); // The JDK default executor owns HTTP work, never the Kafka polling thread.
        started = true;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                send(exchange, 405, "text/plain; charset=utf-8", "Method not allowed".getBytes(StandardCharsets.UTF_8));
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if ("/metrics/dashboard".equals(path)) {
                try {
                    byte[] body = JSON.writeValueAsBytes(aggregator.dashboardSnapshot());
                    send(exchange, 200, "application/json; charset=utf-8", body);
                } catch (RuntimeException | com.fasterxml.jackson.core.JsonProcessingException failure) {
                    System.err.println("Dashboard metrics response failed: " + failure);
                    send(exchange, 500, "application/json; charset=utf-8",
                            "{\"error\":\"Dashboard metrics unavailable\"}".getBytes(StandardCharsets.UTF_8));
                }
                return;
            }
            String resource = RESOURCES.get(path);
            if (resource == null) {
                send(exchange, 404, "text/plain; charset=utf-8", "Not found".getBytes(StandardCharsets.UTF_8));
                return;
            }
            try (InputStream source = DashboardController.class.getResourceAsStream("/static/" + resource)) {
                if (source == null) {
                    send(exchange, 404, "text/plain; charset=utf-8", "Resource unavailable".getBytes(StandardCharsets.UTF_8));
                    return;
                }
                String type = resource.endsWith(".html") ? "text/html; charset=utf-8"
                        : resource.endsWith(".css") ? "text/css; charset=utf-8" : "application/javascript; charset=utf-8";
                send(exchange, 200, type, source.readAllBytes());
            }
        }
    }

    private static void send(HttpExchange exchange, int status, String type, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", type);
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop(0);
        }
    }
}
