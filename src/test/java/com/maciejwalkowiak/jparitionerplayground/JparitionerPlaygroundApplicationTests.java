package com.maciejwalkowiak.jparitionerplayground;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class JparitionerPlaygroundApplicationTests {

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JdbcPartitionRepository jdbcPartitionRepository;
    @Autowired
    private Partitions partitions;

    @BeforeEach
    void setUp() {
        String sql = """
                CREATE TABLE events (
                    id SERIAL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    primary key (id, created_at)
                ) PARTITION BY RANGE (created_at);
                """;

        jdbcClient.sql(sql).update();
    }

    @AfterEach
    void tearDown() {
        jdbcClient.sql("drop table events cascade").update();
    }

    @Test
    void daily() {
        createPartition(pointInTime());
        createPartition(pointInTime().minusDays(1));
        createPartition(pointInTime().minusDays(10));

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .retention(3, RetentionPolicy.DETACH)
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

        jdbcClient.sql("insert into events(name, created_at) values ('xxx', :timestamp)")
                .param("timestamp", LocalDateTime.of(2024, 2, 8, 12, 12, 12))
                .update();

        jdbcClient.sql("insert into events(name, created_at) values ('yyy', :timestamp)")
                .param("timestamp", LocalDateTime.of(2024, 2, 11, 12, 12, 12))
                .update();

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

    @Test
    void monthly() {
        createMonthlyPartition(pointInTime());
        createMonthlyPartition(pointInTime().minusMonths(1));
        createMonthlyPartition(pointInTime().minusMonths(10));

        partitions.refresh(pointInTime(), PartitionConfig.forTable("events")
                .rangeType(RangeType.MONTHLY)
                .retention(2, RetentionPolicy.DETACH)
                .buffer(3));

        var result = jdbcPartitionRepository.findPartitions("events");

        assertThat(result).containsExactlyInAnyOrder(
                Partition.of("events_202402"),
                Partition.of("events_202403"),
                Partition.of("events_202404"),
                Partition.of("events_202401"),
                Partition.of("events_202312")
        );

        jdbcClient.sql("insert into events(name, created_at) values ('xxx', :timestamp)")
                .param("timestamp", LocalDateTime.of(2024, 2, 8, 12, 12, 12))
                .update();

        jdbcClient.sql("insert into events(name, created_at) values ('yyy', :timestamp)")
                .param("timestamp", LocalDateTime.of(2024, 4, 11, 12, 12, 12))
                .update();

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
        jdbcClient.sql("CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')".formatted(day.format(DateTimeFormatter.ofPattern("yyyyMM")), start.format(DateTimeFormatter.ISO_DATE_TIME), start.plusMonths(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME))).update();
    }

    private void createPartition(LocalDate day) {
        jdbcClient.sql("CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')".formatted(day.format(DateTimeFormatter.ofPattern("yyyyMMdd")), day.atStartOfDay().format(DateTimeFormatter.ISO_DATE_TIME), day.atStartOfDay().plusDays(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME))).update();
    }

}

//