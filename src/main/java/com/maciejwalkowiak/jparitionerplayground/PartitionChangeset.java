package com.maciejwalkowiak.jparitionerplayground;

import java.util.List;

public record PartitionChangeset(List<Partition> remove, List<Partition> add) {
}
