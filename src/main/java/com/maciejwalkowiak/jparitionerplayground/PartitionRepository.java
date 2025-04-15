package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.util.List;

public interface PartitionRepository {
    List<PartitionInfo> findPartitions(String tableName);
    void detachPartitions(String tableName, List<PartitionInfo> partitions);
    void createPartitions(String tableName, List<LocalDate> dates);
}
