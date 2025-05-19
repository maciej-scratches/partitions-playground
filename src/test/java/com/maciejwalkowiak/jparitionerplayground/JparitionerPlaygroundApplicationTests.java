package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@Transactional(propagation = Propagation.NEVER)
class JparitionerPlaygroundApplicationTests {

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JdbcPartitionRepository jdbcPartitionRepository;
    @Autowired
    private Partitions partitions;
    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        executeSql("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    primary key (id, created_at)
                ) PARTITION BY RANGE (created_at);
                """);
    }

    @AfterEach
    void tearDown() {
        executeSql("drop schema public cascade");
        executeSql("create schema public");
    }

    @ParameterizedTest
    @EnumSource(RetentionPolicy.class)
    void detachesOrDeletesDailyPartitions(RetentionPolicy retentionPolicy) {
        createDailyPartition(pointInTime());
        createDailyPartition(pointInTime().minusDays(1));
        createDailyPartition(pointInTime().minusDays(10));
        createDailyPartition(pointInTime().minusDays(11));

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .retention(3, retentionPolicy)
                .buffer(4));

        var result = jdbcPartitionRepository.findPartitions("events");

        assertThat(result).containsExactlyInAnyOrder(
                Partition.of("events_20240210"),
                Partition.of("events_20240211"),
                Partition.of("events_20240212"),
                Partition.of("events_20240213"),
                Partition.of("events_20240209"),
                Partition.of("events_20240208"),
                Partition.of("events_20240207")
        );

        if (retentionPolicy == RetentionPolicy.DETACH) {
            assertThat(findTableByName("events_20240131")).isPresent();
            assertThat(findTableByName("events_20240130")).isPresent();
        } else if (retentionPolicy == RetentionPolicy.DROP) {
            assertThat(findTableByName("events_20240131")).isNotPresent();
            assertThat(findTableByName("events_20240130")).isNotPresent();
        } else {
            fail("Unexpected retention policy: " + retentionPolicy);
        }
    }

    @ParameterizedTest
    @EnumSource(RetentionPolicy.class)
    void detachesOrDeletesMonthlyPartitions(RetentionPolicy retentionPolicy) {
        createMonthlyPartition(pointInTime());
        createMonthlyPartition(pointInTime().minusMonths(1));
        createMonthlyPartition(pointInTime().minusMonths(9));
        createMonthlyPartition(pointInTime().minusMonths(10));

        assertThat(jdbcPartitionRepository.findPartitions("events")).containsExactlyInAnyOrder(
                Partition.of("events_202304"),
                Partition.of("events_202305"),
                Partition.of("events_202401"),
                Partition.of("events_202402")
        );

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .rangeType(RangeType.MONTHLY)
                .retention(2, retentionPolicy)
                .buffer(3));

        var result = jdbcPartitionRepository.findPartitions("events");

        assertThat(result).containsExactlyInAnyOrder(
                Partition.of("events_202402"),
                Partition.of("events_202403"),
                Partition.of("events_202404"),
                Partition.of("events_202401"),
                Partition.of("events_202312")
        );

        if (retentionPolicy == RetentionPolicy.DETACH) {
            assertThat(findTableByName("events_202304")).isPresent();
            assertThat(findTableByName("events_202305")).isPresent();
        } else if (retentionPolicy == RetentionPolicy.DROP) {
            assertThat(findTableByName("events_202304")).isNotPresent();
            assertThat(findTableByName("events_202305")).isNotPresent();
        } else {
            fail("Unexpected retention policy: " + retentionPolicy);
        }
    }

    private Optional<String> findTableByName(String tableName) {
        return jdbcClient.sql("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = :tableName")
                .param("tableName", tableName)
                .query(String.class)
                .optional();
    }

    @ParameterizedTest
    @EnumSource(RetentionPolicy.class)
    void insertsRowsToDailyPartitions(RetentionPolicy retentionPolicy) {
        createDailyPartition(pointInTime());
        createDailyPartition(pointInTime().minusDays(1));
        createDailyPartition(pointInTime().minusDays(10));
        createDailyPartition(pointInTime().minusDays(11));

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .retention(3, retentionPolicy)
                .buffer(4));

        transactionTemplate.executeWithoutResult(status -> {
            jdbcClient.sql("insert into events(name, created_at) values ('xxx', :timestamp)")
                    .param("timestamp", LocalDateTime.of(2024, 2, 8, 12, 12, 12))
                    .update();

            jdbcClient.sql("insert into events(name, created_at) values ('yyy', :timestamp)")
                    .param("timestamp", LocalDateTime.of(2024, 2, 11, 12, 12, 12))
                    .update();
        });

        assertThat(jdbcClient.sql("select count(*) from events")
                .query(Long.class)
                .single()).isEqualTo(2);

        assertThat(jdbcClient.sql("select name from events_20240208")
                .query(String.class)
                .single()).isEqualTo("xxx");

        assertThat(jdbcClient.sql("select name from events_20240211")
                .query(String.class)
                .single()).isEqualTo("yyy");
    }

    @ParameterizedTest
    @EnumSource(RetentionPolicy.class)
    void insertsRowsToMonthlyPartitions(RetentionPolicy retentionPolicy) {
        createMonthlyPartition(pointInTime());
        createMonthlyPartition(pointInTime().minusMonths(1));
        createMonthlyPartition(pointInTime().minusMonths(9));
        createMonthlyPartition(pointInTime().minusMonths(10));

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .rangeType(RangeType.MONTHLY)
                .retention(2, retentionPolicy)
                .buffer(3));

        transactionTemplate.executeWithoutResult(status -> {
            jdbcClient.sql("insert into events(name, created_at) values ('xxx', :timestamp)")
                    .param("timestamp", LocalDateTime.of(2024, 2, 8, 12, 12, 12))
                    .update();

            jdbcClient.sql("insert into events(name, created_at) values ('yyy', :timestamp)")
                    .param("timestamp", LocalDateTime.of(2024, 4, 11, 12, 12, 12))
                    .update();
        });

        assertThat(jdbcClient.sql("select count(*) from events")
                .query(Long.class)
                .single()).isEqualTo(2);

        assertThat(jdbcClient.sql("select name from events_202402")
                .query(String.class)
                .single()).isEqualTo("xxx");

        assertThat(jdbcClient.sql("select name from events_202404")
                .query(String.class)
                .single()).isEqualTo("yyy");
    }

    private static LocalDate pointInTime() {
        return LocalDate.of(2024, 2, 10);
    }

    private void createMonthlyPartition(LocalDate day) {
        LocalDateTime start = day.withDayOfMonth(1).atStartOfDay();

        executeSql("CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')".formatted(day.format(DateTimeFormatter.ofPattern("yyyyMM")), start.format(DateTimeFormatter.ISO_DATE_TIME), start.plusMonths(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME)));
    }

    private void createDailyPartition(LocalDate day) {
        executeSql("CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')".formatted(day.format(DateTimeFormatter.ofPattern("yyyyMMdd")), day.atStartOfDay().format(DateTimeFormatter.ISO_DATE_TIME), day.atStartOfDay().plusDays(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME)));
    }

    private void executeSql(String sql) {
        transactionTemplate.executeWithoutResult(status -> {
            jdbcClient.sql(sql).update();
        });
    }
}