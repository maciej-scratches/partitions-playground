package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.util.List;

public interface PartitionRepository {
    List<PartitionRow> findPartitions(String tableName);
    void detachPartitions(String tableName, List<PartitionRow> partitions);
    void createPartitions(String tableName, List<PartitionInfo> dates);
}
