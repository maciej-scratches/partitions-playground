package com.maciejwalkowiak.jparitionerplayground;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link JdbcClient} based implementation of {@link PartitionRepository}.
 *
 * @author Maciej Walkowiak
 */
@Component
public class JdbcPartitionRepository implements PartitionRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcPartitionRepository.class);

    private final JdbcClient jdbcClient;

    public JdbcPartitionRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public List<Partition> findPartitions(String tableName) {
        Assert.notNull(tableName, "tableName must not be null");

        String sql = """
                SELECT
                    child.relname AS name
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

        LOGGER.debug("Executing SQL: {}", sql);

        return jdbcClient.sql(sql)
                .param("tableName", tableName)
                .query((rs, rowNum) -> Partition.of(rs.getString("name")))
                .list()
                .stream()
                .sorted(Comparator.comparing(Partition::name))
                .toList();
    }

    @Override
    public void detachPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        String sql = partitions.stream()
                .map(partition -> "ALTER TABLE " + partition.parentTableName() + " DETACH PARTITION " + partition.name() + ";")
                .collect(Collectors.joining("\n"));

        LOGGER.debug("Executing SQL: {}", sql);

        jdbcClient.sql(sql).update();

    }

    @Override
    public void dropPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        String sql = partitions.stream()
                .map(partitionName -> "DROP TABLE " + partitionName.name() + ";")
                .collect(Collectors.joining("\n"));

        LOGGER.debug("Executing SQL: {}", sql);

        jdbcClient.sql(sql).update();
    }

    @Override
    public void createPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        String sql = partitions.stream()
                .map(it -> "CREATE TABLE " + it.name() + " PARTITION OF " + it.parentTableName() + " FOR VALUES FROM ('" + it.start().format(DateTimeFormatter.ISO_DATE_TIME) + "') TO ('" + it.end().format(DateTimeFormatter.ISO_DATE_TIME) + "');")
                .collect(Collectors.joining("\n"));

        LOGGER.debug("Executing SQL: {}", sql);

        jdbcClient.sql(sql).update();
    }
}
