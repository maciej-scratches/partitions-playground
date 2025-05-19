package com.maciejwalkowiak.jpartitioner;

import java.util.List;

/**
 * Data access layer for partitions.
 *
 * @author Maciej Walkowiak
 */
public interface PartitionRepository {
    /**
     * Returns a list of existing partitions for a given parent table.
     *
     * @param tableName - parent table name
     * @return list of partitions
     */
    List<Partition> findPartitions(String tableName);

    /**
     * Detaches partitions from the parent table.
     *
     * @param partitions - list of partitions to detach
     */
    void detachPartitions(List<Partition> partitions);

    /**
     * Detaches and drops permanently partitions.
     *
     * @param partitions - list of partitions to drop
     */
    void dropPartitions(List<Partition> partitions);

    /**
     * Creates partitions and attaches them to the parent table.
     *
     * @param partitions - list of partitions to add.
     */
    void createPartitions(List<Partition> partitions);
}
