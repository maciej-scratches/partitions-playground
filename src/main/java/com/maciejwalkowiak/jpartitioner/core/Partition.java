package com.maciejwalkowiak.jpartitioner.core;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * A database table partition.
 *
 * @author Maciej Walkowiak
 * @param name - full partition name
 * @param rangeType - if partition is created per month or per day - resolved from a partition name
 */
public record Partition(String name, RangeType rangeType) {
    private static final String SEPARATOR = "_";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * Creates a partition instance to be created for a given parent table name, range and point in time.
     *
     * @param parentTableName - parent table name
     * @param rangeType - range type
     * @param date - point in time
     * @return a new partition
     */
    public static Partition of(String parentTableName, RangeType rangeType, LocalDate date) {
        String suffix = switch (rangeType) {
            case DAILY -> date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            case MONTHLY -> date.format(DateTimeFormatter.ofPattern("yyyyMM"));
        };
        return Partition.of(parentTableName + SEPARATOR + suffix);

    }

    /**
     * Creates partition instance from a full partition name. If name does not follow conventions, throws an exception.
     *
     * @param name - full partition name
     * @return a new partition
     */
    public static Partition of(String name) {
        if (!name.contains(SEPARATOR)) {
            throw new IllegalStateException("Partition name: " + name + " does not contain separator: " + SEPARATOR);
        }
        String suffix = suffix(name);
        RangeType rangeType;
        if (suffix.length() == 6) {
            rangeType = RangeType.MONTHLY;
        } else if (suffix.length() == 8) {
            rangeType = RangeType.DAILY;
        } else {
            throw new IllegalStateException(String.format("Invalid partition name: %s", suffix));
        }
        return new Partition(name, rangeType);
    }

    /**
     * Returns parent table name.
     *
     * @return parent table name
     */
    public String parentTableName() {
        return this.name.substring(0, this.name.lastIndexOf(SEPARATOR));
    }

    /**
     * Returns partition suffix, for example for full name log_details_202405 the suffix is 202405
     *
     * @return partition suffix
     */
    public String suffix() {
        return suffix(this.name);
    }

    /**
     * Returns the beginning of partition range.
     *
     * @return beginning of partition range
     */
    public LocalDateTime start() {
        return switch (this.rangeType) {
            case DAILY -> LocalDate.parse(this.suffix(), FORMATTER).atStartOfDay();
            case MONTHLY -> LocalDate.parse(this.suffix() + "01", FORMATTER).atStartOfDay();
        };
    }

    /**
     * Returns the end of partition range.
     *
     * @return end of partition range
     */
    public LocalDateTime end() {
        return switch (this.rangeType) {
            case DAILY -> LocalDate.parse(this.suffix(), FORMATTER).atStartOfDay().plusDays(1);
            case MONTHLY -> LocalDate.parse(this.suffix() + "01", FORMATTER).atStartOfDay().plusMonths(1);
        };
    }

    private static String suffix(String name) {
        return name.substring(name.lastIndexOf(SEPARATOR) + 1);
    }

    void validate(PartitionConfig config) {
        if (!name.startsWith(config.tableName() + SEPARATOR)) {
            throw new IllegalStateException("Partition name '" + name + "' does not start with " + config.tableName() + SEPARATOR);
        }
        try {
            switch (config.rangeType()) {
                case DAILY -> LocalDate.parse(name.substring(config.tableName().length() + 1), FORMATTER);
                case MONTHLY -> LocalDate.parse(name.substring(config.tableName().length() + 1) + "01", FORMATTER);
            };
        } catch (DateTimeParseException e) {
            throw new IllegalStateException("Partition name '" + name + "' contains invalid date format", e);
        }
    }
}
