package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDateTime;

public record AddPartition(String partitionNameSuffix, LocalDateTime start, LocalDateTime end) {
}
