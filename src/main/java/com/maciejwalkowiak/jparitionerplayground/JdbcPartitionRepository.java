package com.maciejwalkowiak.jparitionerplayground;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

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
    public List<PartitionName> findPartitions(String tableName) {
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
                .query(PartitionName.class)
                .list()
                .stream()
                .sorted(Comparator.comparing(PartitionName::value))
                .toList();
    }

    @Override
    public void detachPartitions(String tableName, List<DropPartition> partitions) {
        String sql = partitions.stream()
                .map(DropPartition::partitionName)
                .map(partitionName -> "ALTER TABLE " + tableName + " DETACH PARTITION " + partitionName + ";")
                .collect(Collectors.joining("\n"));
        System.out.println(sql);
        jdbcClient.sql(sql).update();

    }

    @Override
    public void createPartitions(String tableName, List<AddPartition> partitions) {
        String sql = partitions.stream()
                .map(it -> "CREATE TABLE " + tableName + "_" + it.partitionNameSuffix() + " PARTITION OF " + tableName + " FOR VALUES FROM ('" + it.start().format(DateTimeFormatter.ISO_DATE_TIME) + "') TO ('" + it.end().format(DateTimeFormatter.ISO_DATE_TIME) + "');")
                .collect(Collectors.joining("\n"));
        System.out.println(sql);
        jdbcClient.sql(sql).update();
    }
}
