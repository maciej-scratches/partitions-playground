package com.maciejwalkowiak.jparitionerplayground;

import java.util.List;

public record PartitionChange(List<DropPartition> dropPartitions, List<AddPartition> addPartitions) {
}
