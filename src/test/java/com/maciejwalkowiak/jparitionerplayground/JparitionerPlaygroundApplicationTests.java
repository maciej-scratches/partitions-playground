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

        Partition.forTable("events")
                .retention(7)
                .buffer(7)
                .refresh(jdbcPartitionRepository);

        System.out.println(jdbcPartitionRepository.findPartitions("events"));
    }

    private void createPartition(LocalDate day) {
        jdbcClient.sql("CREATE TABLE IF NOT EXISTS events_%s PARTITION OF events FOR VALUES FROM ('%s') TO ('%s')".formatted(day.format(DateTimeFormatter.ofPattern("yyyyMMdd")), day.atStartOfDay().format(DateTimeFormatter.ISO_DATE_TIME), day.atStartOfDay().plusDays(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME))).update();
    }

}

//