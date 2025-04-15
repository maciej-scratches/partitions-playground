package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record PartitionRow(String partitionName) {
    LocalDate date() {
        String dateSuffix = partitionName.substring(partitionName.indexOf("_") + 1);
        if (dateSuffix.length() == 6) {
            dateSuffix += "01";
        }
        return LocalDate.parse(dateSuffix, DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
}
