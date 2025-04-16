package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

public class PartitionConfig {
    private final String tableName;
    private int retention = 7;
    private int buffer = 0;
    private RangeType rangeType = RangeType.DAILY;

    public RangeType rangeType() {
        return rangeType;
    }

    enum RangeType {
        DAILY,
        MONTHLY;
    }

    private PartitionConfig(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        this.tableName = tableName;
    }

    public static PartitionConfig forTable(String tableName) {
        return new PartitionConfig(tableName);
    }

    public PartitionConfig rangeType(RangeType rangeType) {
        this.rangeType = rangeType;
        return this;
    }

    public PartitionConfig retention(int retention) {
        this.retention = retention;
        return this;
    }

    public PartitionConfig buffer(int buffer) {
        this.buffer = buffer;
        return this;
    }

    public String tableName() {
        return tableName;
    }

    List<Partitions.PartitionInfo> expectedPartitionDates(LocalDate date) {
        return IntStream.range(-retention, buffer)
                .mapToObj(i -> new Partitions.PartitionInfo(partitionName(date, i), partitionDate(date, i), rangeStart(date, i), rangeEnd(date, i)))
                .toList();
    }

    private LocalDate partitionDate(LocalDate date, int i) {
        return switch (this.rangeType) {
            case DAILY -> date.plusDays(i);
            case MONTHLY -> date.withDayOfMonth(1).plusMonths(i);
        };
    }

    private LocalDateTime rangeEnd(LocalDate date, int i) {
        return switch (this.rangeType) {
            case DAILY -> date.plusDays(i + 1).atStartOfDay().minusSeconds(1);
            case MONTHLY -> rangeStart(date, i).plusMonths(1).minusSeconds(1);
        };
    }

    private LocalDateTime rangeStart(LocalDate date, int i) {
        return switch (this.rangeType) {
            case DAILY -> date.plusDays(i).atStartOfDay();
            case MONTHLY -> date.plusMonths(i).atStartOfDay().withDayOfMonth(1);
        };
    }

    private String partitionName(LocalDate date, int i) {
        return switch (this.rangeType) {
            case DAILY -> date.plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            case MONTHLY -> date.plusMonths(i).format(DateTimeFormatter.ofPattern("yyyyMM"));
        };
    }
}