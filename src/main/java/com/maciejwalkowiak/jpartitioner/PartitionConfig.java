package com.maciejwalkowiak.jpartitioner;

import org.springframework.util.Assert;

/**
 * Partitions configuration to be used on {@link Partitions#refresh(PartitionConfig)}.
 *
 * @author Maciej Walkowiak
 */
public class PartitionConfig {
    private final String parentTableName;
    private int retention = 7;
    private int buffer = 0;
    private RangeType rangeType = RangeType.DAILY;
    private RetentionPolicy retentionPolicy = RetentionPolicy.DETACH;

    /**
     * Creates a partition config for an existing parent table.
     *
     * @param parentTableName - parent table name
     * @return partition config
     */
    public static PartitionConfig forTable(String parentTableName) {
        return new PartitionConfig(parentTableName);
    }

    private PartitionConfig(String parentTableName) {
        Assert.notNull(parentTableName, "parentTableName must not be null");
        this.parentTableName = parentTableName;
    }

    /**
     * Returns if partitions are split by day or a month.
     *
     * @return range type
     */
    public RangeType rangeType() {
        return rangeType;
    }

    /**
     * Configures partition config to split partitions by day.
     *
     * @return partition config
     */
    public PartitionConfig daily() {
        this.rangeType = RangeType.DAILY;
        return this;
    }

    /**
     * Configures partition config to split partitions by month
     *
     * @return partition config
     */
    public PartitionConfig monthly() {
        this.rangeType = RangeType.MONTHLY;
        return this;
    }

    /**
     * Configures partition config with given range type.
     *
     * @param rangeType - range type
     * @return partition config
     */
    public PartitionConfig rangeType(RangeType rangeType) {
        Assert.notNull(rangeType, "rangeType cannot be null");
        this.rangeType = rangeType;
        return this;
    }

    /**
     * Configures retention - how many past partitions should be maintained. Partitions outside of retention period are either dropped or detached depending on the {@link RetentionPolicy}.
     *
     * @param retention - retention length - how many days or months
     * @param retentionPolicy - policy on what to do with partitions older than retention period
     * @return partition config
     */
    public PartitionConfig retention(int retention, RetentionPolicy retentionPolicy) {
        Assert.notNull(retentionPolicy, "retentionPolicy cannot be null");
        this.retention = retention;
        this.retentionPolicy = retentionPolicy;
        return this;
    }

    /**
     * Configures buffer - how many future partitions should be created upfront.
     *
     * @param buffer - number of how many partitions should be created upfront.
     * @return partition config
     */
    public PartitionConfig buffer(int buffer) {
        this.buffer = buffer;
        return this;
    }

    /**
     * Returns parent table name.
     *
     * @return parent table name
     */
    public String tableName() {
        return parentTableName;
    }

    /**
     * Returns retention.
     *
     * @return retention
     */
    public int retention() {
        return retention;
    }

    /**
     * Returns buffer.
     *
     * @return buffer
     */
    public int buffer() {
        return buffer;
    }

    /**
     * Returns retention policy.
     *
     * @return retention policy
     */
    public RetentionPolicy retentionPolicy() {
        return retentionPolicy;
    }
}