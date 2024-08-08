package com.example.starter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;

@ExtendWith(VertxExtension.class)
public class PostgresTest {

  private static PostgreSQLContainer<?> postgresContainer;
  private static Vertx vertx;
  private static Pool pgPool;

  @BeforeAll
  static void setup() {
    postgresContainer = new PostgreSQLContainer<>("postgres")
      .withDatabaseName("testdb")
      .withUsername("user")
      .withPassword("password");
    postgresContainer.start();

    MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
      .setPrometheusOptions(new VertxPrometheusOptions()
        .setEnabled(true)
        .setStartEmbeddedServer(true)
        .setEmbeddedServerOptions(new io.vertx.core.http.HttpServerOptions().setPort(8081))
        .setPublishQuantiles(true))
      .setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    PgConnectOptions connectOptions = new PgConnectOptions()
      .setPort(postgresContainer.getFirstMappedPort())
      .setHost(postgresContainer.getHost())
      .setDatabase(postgresContainer.getDatabaseName())
      .setUser(postgresContainer.getUsername())
      .setPassword(postgresContainer.getPassword());

    PoolOptions poolOptions = new PoolOptions().setMaxSize(5);

    pgPool = PgBuilder.pool()
      .with(poolOptions)
      .connectingTo(connectOptions)
      .using(vertx)
      .build();
  }


  @Test
  //while the test is running you can scrape the metrics on http://localhost:8081/metrics
  //you will find vertx_sql_processing_time_seconds_bucket and vertx_sql_processing_pending metrics
  //but not vertx_sql_resets_total or vertx_sql_queue_pending
  void testDatabaseConnection(VertxTestContext testContext) {
    for (int i = 0; i < 1_000_000; i++) {
      pgPool.query("SELECT 1").execute(ar -> {
        if (ar.succeeded()) {
          Row row = ar.result().iterator().next();
          assertEquals(1, row.getInteger(0));
        } else {
        }
      });
    }
//    testContext.completeNow();
  }

  @AfterAll
  static void tearDown() {
    if (pgPool != null) {
      pgPool.close();
    }
    if (vertx != null) {
      vertx.close();
    }
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
  }

}
