package com.maciejwalkowiak.jparitionerplayground;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * {@link JdbcTemplate} based implementation of {@link PartitionRepository}.
 *
 * @author Maciej Walkowiak
 */
@Component
public class JdbcPartitionRepository implements PartitionRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcPartitionRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public JdbcPartitionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
                    parent.relname = ?
                """;

        LOGGER.debug("Executing SQL: {}", sql);

        return jdbcTemplate.query(sql, (rs, rowNum) -> Partition.of(rs.getString("name")), tableName)
                .stream()
                .sorted(Comparator.comparing(Partition::name))
                .toList();
    }

    @Override
    public void detachPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        partitions.stream()
                .map(partition -> "ALTER TABLE " + partition.parentTableName() + " DETACH PARTITION " + partition.name() + " CONCURRENTLY")
                .forEach(this::executeWithAutoCommitEnabled);
    }

    @Override
    public void dropPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        // first detach partitions concurrently
        detachPartitions(partitions);

        // delete only partitions that are already detached
        partitions.stream()
                .map(Partition::parentTableName)
                .distinct()
                .flatMap(this::findDetachedPartitionNames)
                .map(partitionName -> "DROP TABLE " + partitionName)
                .forEach(this::executeWithAutoCommitEnabled);
    }

    @Override
    public void createPartitions(List<Partition> partitions) {
        Assert.notNull(partitions, "partitions must not be null");

        partitions.stream()
                .map(it -> "CREATE TABLE " + it.name() + " PARTITION OF " + it.parentTableName() + " FOR VALUES FROM ('" + it.start().format(DateTimeFormatter.ISO_DATE_TIME) + "') TO ('" + it.end().format(DateTimeFormatter.ISO_DATE_TIME) + "')")
                .forEach(this::executeWithAutoCommitEnabled);
    }

    private void executeWithAutoCommitEnabled(String sql) {
        LOGGER.info("Executing SQL: {}", sql);

        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            boolean originalAutoCommit = connection.getAutoCommit();

            try {
                connection.setAutoCommit(true);
                try (Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to execute SQL statement", e);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
            return null;
        });
    }

    private Stream<String> findDetachedPartitionNames(String parentTableName) {
        return jdbcTemplate.queryForList("""
                        SELECT
                            c.relname AS name
                        FROM
                            pg_class c
                        JOIN
                            pg_namespace ns ON c.relnamespace = ns.oid
                        WHERE
                            c.relkind = 'r'
                          AND c.relispartition IS false 
                          AND c.relname LIKE ? || '%'""", String.class, parentTableName)
                .stream();
    }
}
