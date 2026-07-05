package com.cinemaabyss.events;

public record AppConfig(int port, String kafkaBrokers) {
    public static AppConfig fromEnvironment() {
        return new AppConfig(
                intValue("PORT", 8082),
                stringValue("KAFKA_BROKERS", "localhost:9092")
        );
    }

    private static String stringValue(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int intValue(String name, int fallback) {
        try {
            return Integer.parseInt(stringValue(name, String.valueOf(fallback)));
        } catch (NumberFormatException error) {
            return fallback;
        }
    }
}
