package com.maciejwalkowiak.jparitionerplayground;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;

@Component
public class Partitions {
    private final PartitionRepository partitionRepository;

    public Partitions(PartitionRepository partitionRepository) {
        this.partitionRepository = partitionRepository;
    }

    private PartitionChange diff(LocalDate date, PartitionConfig config) {
        var partitions = partitionRepository.findPartitions(config.tableName());
        validateNamingPattern(config, partitions);

        var currentPartitionDates = config.expectedPartitionDates(date);

        var partitionsToDrop = partitions
                .stream()
                .filter(it -> currentPartitionDates.stream().noneMatch(p -> p.date().equals(it.date())))
                .map(it -> new DropPartition(it.value()))
                .toList();

        var partitionsToAdd = currentPartitionDates.stream()
                .filter(it -> partitions.stream().noneMatch(partition -> partition.date().equals(it.date())))
                .map(it -> new AddPartition(it.partitionName(), it.start(), it.end()))
                .sorted(Comparator.comparing(AddPartition::partitionNameSuffix))
                .toList();
        return new PartitionChange(partitionsToDrop, partitionsToAdd);
    }

    public void refresh(PartitionConfig config) {
        refresh(LocalDate.now(), config);
    }
    public void refresh(LocalDate date, PartitionConfig config) {
        var diff = diff(date, config);
        partitionRepository.detachPartitions(config.tableName(), diff.dropPartitions());
        partitionRepository.createPartitions(config.tableName(), diff.addPartitions());
    }

    private void validateNamingPattern(PartitionConfig config, List<PartitionName> partitions) {
        for (PartitionName partition : partitions) {
            if (!partition.value().startsWith(config.tableName() + "_")) {
                throw new IllegalStateException("Partition name '" + partition.value() + "' does not start with " + config.tableName() + "_");
            }
            try {
                switch (config.rangeType()) {
                    case DAILY -> LocalDate.parse(partition.value().substring(config.tableName().length() + 1), DateTimeFormatter.ofPattern("yyyyMMdd"));
                    case MONTHLY -> LocalDate.parse(partition.value().substring(config.tableName().length() + 1) + "01", DateTimeFormatter.ofPattern("yyyyMMdd"));
                };
            } catch (DateTimeParseException e) {
                throw new IllegalStateException("Partition name '" + partition.value() + "' contains invalid date format", e);
            }
        }
    }

    public record PartitionInfo(String partitionName, LocalDate date, LocalDateTime start, LocalDateTime end) {}
}
