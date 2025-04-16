package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record PartitionName(String value) {
    LocalDate date() {
        String dateSuffix = value.substring(value.indexOf("_") + 1);
        if (dateSuffix.length() == 6) {
            dateSuffix += "01";
        }
        return LocalDate.parse(dateSuffix, DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
}
