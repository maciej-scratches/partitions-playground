package com.maciejwalkowiak.jparitionerplayground;

public class PartitionConfig {
    private final String tableName;
    private int retention = 7;
    private int buffer = 0;
    private RangeType rangeType = RangeType.DAILY;
    private RetentionPolicy retentionPolicy = RetentionPolicy.DETACH;

    public static PartitionConfig forTable(String tableName) {
        return new PartitionConfig(tableName);
    }

    public RangeType rangeType() {
        return rangeType;
    }

    private PartitionConfig(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        this.tableName = tableName;
    }

    public PartitionConfig daily() {
        this.rangeType = RangeType.DAILY;
        return this;
    }

    public PartitionConfig monthly() {
        this.rangeType = RangeType.MONTHLY;
        return this;
    }

    public PartitionConfig rangeType(RangeType rangeType) {
        this.rangeType = rangeType;
        return this;
    }

    public PartitionConfig retention(int retention, RetentionPolicy retentionPolicy) {
        this.retention = retention;
        this.retentionPolicy = retentionPolicy;
        return this;
    }

    public PartitionConfig retention(int retention) {
        this.retention(retention, RetentionPolicy.DETACH);
        return this;
    }

    public PartitionConfig buffer(int buffer) {
        this.buffer = buffer;
        return this;
    }

    public String tableName() {
        return tableName;
    }

    public int retention() {
        return retention;
    }

    public int buffer() {
        return buffer;
    }

    public RetentionPolicy retentionPolicy() {
        return retentionPolicy;
    }
}