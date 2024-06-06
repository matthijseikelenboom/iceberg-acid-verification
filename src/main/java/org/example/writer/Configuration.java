package org.example.writer;

import lombok.*;

@NoArgsConstructor(staticName = "create")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Configuration {
    public static final String DEFAULT_CATALOG_NAME = "iceberghive";
    public static final String DEFAULT_DATABASE_NAME = "concurrencytestdb";
    public static final String DEFAULT_TABLE_NAME = "acid_verification";
    public static final int DEFAULT_NUMBER_OF_WRITERS_THREADS = 4;
    public static final int DEFAULT_NUMBER_OF_READER_THREADS = 2;
    public static final int DEFAULT_NUMBER_OF_SPARK_SESSIONS_FOR_WRITERS = 4;
    public static final int DEFAULT_NUMBER_OF_SPARK_SESSIONS_FOR_READERS = 2;
    public static final int DEFAULT_TOTAL_NUMBERS_OF_TRANSACTIONS = 1000;
    public static final int DEFAULT_MAXIMUM_NUMBER_OF_RECORDS = 100;
    public static final int DEFAULT_MAXIMUM_NUMBER_OF_PARTITIONS = 4;
    public static final int DEFAULT_RECORDS_PER_TRANSACTION = 3;
    public static final float DEFAULT_PREFERENCE_TO_INSERT_OVER_OTHER_MANIPULATIONS = 0.25F;
    public static final float DEFAULT_PREFERENCE_TO_UPDATE_OVER_DELETE = 0.75F;
    public static final long DEFAULT_RANDOM_SEED = 1234L;

    @With
    private String catalogName = DEFAULT_CATALOG_NAME;

    @With
    private String databaseName = DEFAULT_DATABASE_NAME;

    @With
    private String tableName = DEFAULT_TABLE_NAME;

    @With
    private int numberOfWriterThreads = DEFAULT_NUMBER_OF_WRITERS_THREADS;

    @With
    private int numberOfReaderThreads = DEFAULT_NUMBER_OF_READER_THREADS;

    @With
    private int numberOfSparkSessionsForWriters = DEFAULT_NUMBER_OF_SPARK_SESSIONS_FOR_WRITERS;

    @With
    private int numberOfSparkSessionsForReaders = DEFAULT_NUMBER_OF_SPARK_SESSIONS_FOR_READERS;

    @With
    private int totalNumberOfTransactions = DEFAULT_TOTAL_NUMBERS_OF_TRANSACTIONS;

    @With
    private int maximumNumberOfRecords = DEFAULT_MAXIMUM_NUMBER_OF_RECORDS;

    @With
    private int maximumNumberOfPartitions = DEFAULT_MAXIMUM_NUMBER_OF_PARTITIONS;

    @With
    private int recordsPerTransaction = DEFAULT_RECORDS_PER_TRANSACTION;

    @With
    private float preferenceToInsertOverOtherManipulations = DEFAULT_PREFERENCE_TO_INSERT_OVER_OTHER_MANIPULATIONS;

    @With
    private float preferenceToUpdateOverDelete = DEFAULT_PREFERENCE_TO_UPDATE_OVER_DELETE;

    @With
    private long randomSeed = DEFAULT_RANDOM_SEED;
}
