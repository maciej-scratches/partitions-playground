package com.maciejwalkowiak.jparitionerplayground;

import org.springframework.boot.autoconfigure.jdbc.JdbcClientAutoConfiguration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class JdbcPartitionRepository implements PartitionRepository {
    private final JdbcClient jdbcClient;

    public JdbcPartitionRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<PartitionInfo> findPartitions(String tableName) {
        String sql = """
                SELECT
                    child.relname AS partition_name
                FROM
                    pg_inherits
                JOIN
                    pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN
                    pg_class child ON pg_inherits.inhrelid = child.oid
                JOIN
                    pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
                JOIN
                    pg_namespace child_ns ON child.relnamespace = child_ns.oid
                WHERE
                    parent.relname = :tableName
                """;

        return jdbcClient.sql(sql)
                .param("tableName", tableName)
                .query(PartitionInfo.class)
                .list()
                .stream()
                .sorted(Comparator.comparing(PartitionInfo::partitionName))
                .toList();
    }

    @Override
    public void detachPartitions(String tableName, List<PartitionInfo> partitions) {
        String sql = partitions.stream()
                .map(PartitionInfo::partitionName)
                .map(partitionName -> "ALTER TABLE " + tableName + " DETACH PARTITION " + partitionName + " CONCURRENTLY;")
                .collect(Collectors.joining());
        jdbcClient.sql(sql).update();

    }

    @Override
    public void createPartitions(String tableName, List<LocalDate> dates) {
        String sql = dates.stream()
                .map(date -> "CREATE TABLE " + tableName + "_" + date.format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " PARTITION OF " + tableName + " FOR VALUES FROM ('" + date.atStartOfDay().format(DateTimeFormatter.ISO_DATE_TIME) + "') TO ('" + date.atStartOfDay().plusDays(1).minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME) + "');")
                .collect(Collectors.joining());
        jdbcClient.sql(sql).update();
    }
}
