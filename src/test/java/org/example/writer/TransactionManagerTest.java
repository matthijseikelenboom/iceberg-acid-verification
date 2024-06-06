package org.example.writer;

import org.apache.spark.sql.SparkSession;
import org.example.SparkSessionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionManagerTest {
    private static final Path testDataDir = Path.of("/tmp/lakehouse");
    private static final SparkSessionProvider SPARK_SESSION_PROVIDER = new SparkSessionProvider(testDataDir, true);
    private static final SparkSession session = SPARK_SESSION_PROVIDER.getSession();
    private static Configuration configuration;
    private TransactionManager transactionManager;

    @BeforeAll
    static void cleanInstallDatabase() throws IOException {
        testDataDir.toFile().mkdir();
        FileUtils.cleanDirectory(testDataDir.toFile());

        configuration = Configuration.create()
                .withTotalNumberOfTransactions(100)
                .withNumberOfWriterThreads(2)
                .withNumberOfSparkSessionsForWriters(2);
    }

    @BeforeEach
    void setup() {
        transactionManager = new TransactionManager(configuration, session);
    }

    @Test
    void run() throws InterruptedException {
        // When
        transactionManager.run();

        // Then
        final var dataSet = session.sql(String.format("SELECT * FROM %s.%s", configuration.getDatabaseName(), configuration.getTableName()));
        final var rows = dataSet.collectAsList();
        System.out.println(rows.size());
        assertThat(transactionManager.hasFailedVerification()).withFailMessage("ACID Verification failed.").isFalse();
        assertThat(transactionManager.isHasFailedWriters()).withFailMessage("One or more writer threads failed").isFalse();
        assertThat(transactionManager.isHasFailedReaders()).withFailMessage("One or more reader threads failed").isFalse();
    }

}
