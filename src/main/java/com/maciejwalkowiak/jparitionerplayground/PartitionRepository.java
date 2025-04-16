package com.maciejwalkowiak.jparitionerplayground;

import java.util.List;

public interface PartitionRepository {
    List<PartitionName> findPartitions(String tableName);
    void detachPartitions(String tableName, List<DropPartition> partitions);
    void createPartitions(String tableName, List<AddPartition> dates);
}
