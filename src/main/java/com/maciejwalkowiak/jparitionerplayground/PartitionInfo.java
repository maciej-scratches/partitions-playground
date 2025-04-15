package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record PartitionInfo(String partitionName, LocalDate date, LocalDateTime start, LocalDateTime end) implements Comparable<PartitionInfo> {
    @Override
    public int compareTo(PartitionInfo o) {
        return this.partitionName.compareTo(o.partitionName);
    }
}
