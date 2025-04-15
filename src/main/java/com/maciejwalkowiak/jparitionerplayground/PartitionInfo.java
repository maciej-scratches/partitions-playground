package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record PartitionInfo(String partitionName) {
    LocalDate date() {
        return LocalDate.parse(partitionName.substring(partitionName.indexOf("_") + 1), DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
}
