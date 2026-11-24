package com.terminal.config;

import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.api.FlywayException;
import org.springframework.boot.autoconfigure.flyway.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Keeps a database outage from becoming a <em>startup</em> failure.
 *
 * <h2>The problem</h2>
 * <p>By default Spring Boot runs Flyway during context refresh. If Postgres is unreachable
 * the migration throws and the whole application fails to start — including the read side
 * (trades, candles, depth), which does not need the database at all. That is a strictly
 * worse failure mode than the one it replaces: before this step the service booted happily
 * with a cold database (Hikari is configured with {@code initialization-fail-timeout: -1})
 * and only the endpoints that needed it failed.
 *
 * <h2>The fix</h2>
 * <p>A migration strategy that reports the problem and continues. When the database is
 * available the migration runs exactly as before; when it is not, the service starts,
 * {@code /api/orders} answers {@code 503} (see {@code OrderExceptionHandler}) and every
 * market-data endpoint keeps working.
 *
 * <p>This is not a licence to ignore migrations — a failed migration is logged at ERROR and
 * the schema stays behind, so the write side is genuinely unavailable until it is fixed.
 * The trade-off is deliberate: <b>the trading UI must not go dark because the order
 * database is down.</b> The alternative, in a cluster, is a CrashLoopBackOff that takes the
 * healthy parts of the system down with it.
 *
 * <p>Hibernate is told not to read JDBC metadata at boot
 * ({@code hibernate.boot.allow_jdbc_metadata_access: false} in application.yml, with an
 * explicit dialect), otherwise the {@code EntityManagerFactory} would make the same fatal
 * connection attempt a few beans later.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class PersistenceConfig {

    @Bean
    FlywayMigrationStrategy tolerantMigrationStrategy() {
        return flyway -> {
            try {
                var result = flyway.migrate();
                log.info("Flyway: {} migration(s) applied, schema at version {}",
                        result.migrationsExecuted, result.targetSchemaVersion);
            } catch (FlywayException ex) {
                log.error("""
                        Flyway could not run; the service will start but the write side is \
                        unavailable until the schema exists. Cause: {}""", ex.getMessage());
            }
        };
    }
}
