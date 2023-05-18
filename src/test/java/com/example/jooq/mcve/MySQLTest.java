package com.example.jooq.mcve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.metadata.HikariDataSourcePoolMetadata;
import org.testcontainers.containers.MySQLContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class MySQLTest {

    private static MySQLContainer<?> container;
    private static HikariDataSourcePoolMetadata dataSourceMonitor;
    private static DSLContext context;

    @BeforeAll
    static void setUp() {
        container = new MySQLContainer<>("mysql:latest");
        container.start();

        var hikariConfig = new HikariConfig();
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setMaximumPoolSize(10);

        var dataSource = new HikariDataSource(hikariConfig);
        dataSourceMonitor = new HikariDataSourcePoolMetadata(dataSource);

        var configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.MYSQL);
        configuration.set(new DataSourceConnectionProvider(dataSource));

        context = DSL.using(configuration);
    }

    @AfterAll
    static void tearDown() {
        container.stop();
    }

    @Test
    void cancelSignalShouldReleaseHikariConnection() {
        var previousActive = dataSourceMonitor.getActive();

        // Simulate a slow query
        var statement = context.select(DSL.function("sleep", SQLDataType.INTEGER, DSL.value(6)));

        StepVerifier.create(
            Flux.from(statement)
                .then()
                .timeout(Duration.ofSeconds(3)))
            .expectError(TimeoutException.class)
            .verify();

        // Hikari connection should be released
        assertEquals(previousActive, (int) dataSourceMonitor.getActive());
    }

    @Test
    void manuallyHandlingCancelSignalShouldReleaseHikariConnection() {
        var previousActive = dataSourceMonitor.getActive();

        // Simulate a slow statement
        var statement = context.select(DSL.function("sleep", SQLDataType.INTEGER, DSL.value(6)));

        StepVerifier.create(
                Flux.from(statement)
                    .doOnCancel(statement::cancel) // Handle signal and cancel query
                    .then()
                    .timeout(Duration.ofSeconds(3)))
            .expectError(TimeoutException.class)
            .verify();

        // Hikari connection should be released
        assertEquals(previousActive, (int) dataSourceMonitor.getActive());
    }
}
