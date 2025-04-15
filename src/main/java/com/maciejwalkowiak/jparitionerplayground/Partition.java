package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Partition {
    private final String tableName;

    private Partition(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        this.tableName = tableName;
    }

    public static Partition forTable(String tableName) {
        return new Partition(tableName);
    }

    public RangePartition byRange(String columnName) {
        return new RangePartition(this.tableName, columnName);
    }

    public static class RangePartition {
        private final String tableName;
        private final String columnName;

        private RangePartition(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        public RangeTypedPartition daily() {
            return new RangeTypedPartition(this.tableName, this.columnName, RangeType.DAILY);
        }

        public RangeTypedPartition monthly() {
            return new RangeTypedPartition(this.tableName, this.columnName, RangeType.MONTHLY);
        }
    }

    public static class RangeTypedPartition {
        private final String tableName;
        private final String columnName;
        private final RangeType rangeType;
        private int retention = 7;
        private int buffer = 0;

        private RangeTypedPartition(String tableName, String columnName, RangeType rangeType) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.rangeType = rangeType;
        }

        public RangeTypedPartition retention(int retention) {
            this.retention = retention;
            return this;
        }

        public RangeTypedPartition buffer(int buffer) {
            this.buffer = buffer;
            return this;
        }

        public String toSql() {
            return IntStream.range(0, buffer)
                    .mapToObj(i -> "CREATE TABLE " + this.tableName + "_" + partitionName(i) + " PARTITION OF " + tableName + " FOR VALUES FROM ('" + rangeStart(i).format(DateTimeFormatter.ISO_DATE_TIME) + "') TO ('" + rangeEnd(i).format(DateTimeFormatter.ISO_DATE_TIME) + "');")
                    .collect(Collectors.joining("\n"));
        }

        private LocalDateTime rangeEnd(int i) {
            LocalDate now = LocalDate.now();
            return switch (this.rangeType) {
                case DAILY -> now.plusDays(i + 1).atStartOfDay().minusSeconds(1);
                case MONTHLY -> rangeStart(i).plusMonths(1).minusSeconds(1);
            };
        }

        private LocalDateTime rangeStart(int i) {
            LocalDate now = LocalDate.now();
            return switch (this.rangeType) {
                case DAILY -> now.plusDays(i).atStartOfDay();
                case MONTHLY -> now.plusMonths(i).atStartOfDay().withDayOfMonth(1);
            };
        }

        private String partitionName(int i) {
            LocalDate now = LocalDate.now();
            return switch (this.rangeType) {
                case DAILY -> now.plusDays(i).format(DateTimeFormatter.ISO_DATE);
                case MONTHLY -> now.plusMonths(i).format(DateTimeFormatter.ofPattern("yyyy-MM"));
            };
        }
    }

    private enum RangeType {
        DAILY,
        MONTHLY
    }
}
