package com.cinemaabyss.proxy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class ProxyServiceApplication {
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            "connection",
            "content-length",
            "expect",
            "host",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade"
    );

    private final AppConfig config;
    private final HttpClient httpClient;

    public ProxyServiceApplication(AppConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public static void main(String[] args) throws IOException {
        AppConfig config = AppConfig.fromEnvironment();
        ProxyServiceApplication application = new ProxyServiceApplication(config);

        HttpServer server = HttpServer.create(new InetSocketAddress(config.port()), 0);
        server.createContext("/health", application::handleHealth);
        server.createContext("/", application::handleProxy);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        System.out.printf("Starting Java proxy service on port %d%n", config.port());
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        sendJson(exchange, 200, "{\"status\":true}");
    }

    private void handleProxy(HttpExchange exchange) throws IOException {
        String targetBaseUrl = selectTarget(exchange.getRequestURI().getPath());
        URI targetUri = buildTargetUri(targetBaseUrl, exchange.getRequestURI());

        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(targetUri)
                    .timeout(Duration.ofSeconds(30))
                    .method(exchange.getRequestMethod(), requestBody(exchange));

            copyRequestHeaders(exchange.getRequestHeaders(), requestBuilder);
            requestBuilder.header("X-Forwarded-Host", exchange.getRequestHeaders().getFirst("Host") == null
                    ? ""
                    : exchange.getRequestHeaders().getFirst("Host"));
            requestBuilder.header("X-Gateway", "cinemaabyss-proxy");

            HttpResponse<byte[]> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
            copyResponseHeaders(response, exchange.getResponseHeaders());

            byte[] responseBody = response.body();
            exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(responseBody);
            }

            System.out.printf("%s %s -> %s %d%n",
                    exchange.getRequestMethod(),
                    exchange.getRequestURI(),
                    targetBaseUrl,
                    response.statusCode());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            sendJson(exchange, 502, "{\"error\":\"Upstream service unavailable\"}");
        } catch (Exception error) {
            System.err.printf("Upstream request failed: %s%n", error.getMessage());
            sendJson(exchange, 502, "{\"error\":\"Upstream service unavailable\"}");
        }
    }

    private String selectTarget(String path) {
        if (path.startsWith("/api/events")) {
            return config.eventsServiceUrl();
        }

        if (path.startsWith("/api/movies")) {
            if (!config.gradualMigration()) {
                return config.monolithUrl();
            }

            int bucket = ThreadLocalRandom.current().nextInt(100);
            if (bucket < config.moviesMigrationPercent()) {
                return config.moviesServiceUrl();
            }
            return config.monolithUrl();
        }

        return config.monolithUrl();
    }

    private URI buildTargetUri(String targetBaseUrl, URI requestUri) {
        StringBuilder uri = new StringBuilder(targetBaseUrl);
        uri.append(requestUri.getRawPath());
        if (requestUri.getRawQuery() != null) {
            uri.append('?').append(requestUri.getRawQuery());
        }
        return URI.create(uri.toString());
    }

    private HttpRequest.BodyPublisher requestBody(HttpExchange exchange) throws IOException {
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        if (requestBody.length == 0) {
            return HttpRequest.BodyPublishers.noBody();
        }
        return HttpRequest.BodyPublishers.ofByteArray(requestBody);
    }

    private void copyRequestHeaders(Headers source, HttpRequest.Builder target) {
        source.forEach((name, values) -> {
            if (HOP_BY_HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                return;
            }
            for (String value : values) {
                target.header(name, value);
            }
        });
    }

    private void copyResponseHeaders(HttpResponse<byte[]> response, Headers target) {
        response.headers().map().forEach((name, values) -> {
            if (!HOP_BY_HOP_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                target.put(name, List.copyOf(values));
            }
        });
    }

    private void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] responseBody = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(responseBody);
        }
    }
}
