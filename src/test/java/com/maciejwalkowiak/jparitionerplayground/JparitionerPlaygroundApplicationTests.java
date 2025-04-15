package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class JparitionerPlaygroundApplicationTests {

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JdbcPartitionRepository jdbcPartitionRepository;
    @Autowired
    private Partitions partitions;

    @Test
    void contextLoads() {
        String sql = """
                CREATE TABLE events (
                    id SERIAL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    primary key (id, created_at)
                ) PARTITION BY RANGE (created_at);
                """;

        jdbcClient.sql(sql).update();

        createPartition(LocalDate.now());
        createPartition(LocalDate.now().minusDays(1));
        createPartition(LocalDate.now().minusDays(10));

        System.out.println(jdbcPartitionRepository.findPartitions("events"));

        partitions.refresh(PartitionConfig.forTable("events")
                .retention(7)
                .buffer(7));

        System.out.println(jdbcPartitionRepository.findPartitions("events"));
    }

    @Test
    void monthly() {
        String sql = """
                CREATE TABLE events (
                    id SERIAL,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    primary key (id, created_at)
                ) PARTITION BY RANGE (created_at);
                """;

        jdbcClient.sql(sql).update();

        createMonthlyPartition(LocalDate.now());
        createMonthlyPartition(LocalDate.now().minusMonths(1));
        createMonthlyPartition(LocalDate.now().minusMonths(10));

        System.out.println(jdbcPartitionRepository.findPartitions("events"));

        partitions.refresh(PartitionConfig.forTable("events")
                        .rangeType(PartitionConfig.RangeType.MONTHLY)
                .retention(2)
                .buffer(3));

        System.out.println(jdbcPartitionRepository.findPartitions("events"));
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