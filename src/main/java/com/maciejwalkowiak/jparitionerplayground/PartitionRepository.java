package com.maciejwalkowiak.jparitionerplayground;

import java.util.List;

public interface PartitionRepository {
    List<Partition> findPartitions(String tableName);
    void detachPartitions(List<Partition> partitions);
    void dropPartitions(List<Partition> partitions);
    void createPartitions(List<Partition> partitions);
}
