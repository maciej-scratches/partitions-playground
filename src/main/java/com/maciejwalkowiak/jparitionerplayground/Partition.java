package com.maciejwalkowiak.jparitionerplayground;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.stream.IntStream;

public class Partition {
    private final String tableName;
    private int retention = 7;
    private int buffer = 0;

    private Partition(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        this.tableName = tableName;
    }

    public static Partition forTable(String tableName) {
        return new Partition(tableName);
    }

    public Partition retention(int retention) {
        this.retention = retention;
        return this;
    }

    public Partition buffer(int buffer) {
        this.buffer = buffer;
        return this;
    }

    public void refresh(PartitionRepository partitionRepository) {
        var partitions = partitionRepository.findPartitions(this.tableName);
        validateNamingPattern(partitions);

        var currentPartitionDates = this.currentPartitionDates();

        partitionRepository.detachPartitions(this.tableName, partitions
                .stream()
                .filter(it -> !currentPartitionDates.contains(it.date()))
                .toList());

        partitionRepository.createPartitions(this.tableName, currentPartitionDates.stream()
                .filter(it -> partitions.stream().noneMatch(partition -> partition.date().equals(it)))
                .sorted()
                .toList());
    }

    private List<LocalDate> currentPartitionDates() {
        return IntStream.range(-retention, buffer)
                .mapToObj(i -> LocalDate.now().plusDays(i))
                .toList();
    }

    private void validateNamingPattern(List<PartitionInfo> partitions) {
        for (PartitionInfo partition : partitions) {
            if (!partition.partitionName().startsWith(this.tableName + "_")) {
                throw new IllegalArgumentException("Partition name '" + partition.partitionName() + "' does not start with " + this.tableName + "_");
            }
            try {
                LocalDate.parse(partition.partitionName().substring(this.tableName.length() + 1), DateTimeFormatter.ofPattern("yyyyMMdd"));
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Partition name '" + partition.partitionName() + "' contains invalid date format");
            }
        }
    }
}