package com.cinemaabyss.proxy;

public record AppConfig(
        int port,
        String monolithUrl,
        String moviesServiceUrl,
        String eventsServiceUrl,
        boolean gradualMigration,
        int moviesMigrationPercent
) {
    public static AppConfig fromEnvironment() {
        return new AppConfig(
                intValue("PORT", 8000),
                stringValue("MONOLITH_URL", "http://localhost:8080"),
                stringValue("MOVIES_SERVICE_URL", "http://localhost:8081"),
                stringValue("EVENTS_SERVICE_URL", "http://localhost:8082"),
                Boolean.parseBoolean(stringValue("GRADUAL_MIGRATION", "false")),
                clampPercent(intValue("MOVIES_MIGRATION_PERCENT", 0))
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

    private static int clampPercent(int value) {
        if (value < 0) {
            return 0;
        }
        return Math.min(value, 100);
    }
}
