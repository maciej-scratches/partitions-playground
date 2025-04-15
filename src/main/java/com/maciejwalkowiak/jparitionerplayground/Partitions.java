package com.maciejwalkowiak.jparitionerplayground;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

@Component
public class Partitions {
    private final PartitionRepository partitionRepository;

    public Partitions(PartitionRepository partitionRepository) {
        this.partitionRepository = partitionRepository;
    }

    public void refresh(PartitionConfig config) {
        refresh(LocalDate.now(), config);
    }
    public void refresh(LocalDate date, PartitionConfig config) {
        var partitions = partitionRepository.findPartitions(config.tableName());
        validateNamingPattern(config, partitions);

        var currentPartitionDates = config.expectedPartitionDates(date);

        partitionRepository.detachPartitions(config.tableName(), partitions
                .stream()
                .filter(it -> currentPartitionDates.stream().noneMatch(p -> p.date().equals(it.date())))
                .toList());

        partitionRepository.createPartitions(config.tableName(), currentPartitionDates.stream()
                .filter(it -> partitions.stream().noneMatch(partition -> partition.date().equals(it.date())))
                .sorted()
                .toList());
    }

    private void validateNamingPattern(PartitionConfig config, List<PartitionRow> partitions) {
        for (PartitionRow partition : partitions) {
            if (!partition.partitionName().startsWith(config.tableName() + "_")) {
                throw new IllegalArgumentException("Partition name '" + partition.partitionName() + "' does not start with " + config.tableName() + "_");
            }
            try {
                DateTimeFormatter formatter = switch (config.rangeType()) {
                    case DAILY -> DateTimeFormatter.ofPattern("yyyyMMdd");
                    case MONTHLY -> DateTimeFormatter.ofPattern("yyyyMMdd");
                };
                LocalDate.parse(partition.partitionName().substring(config.tableName().length() + 1) + "01", formatter);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Partition name '" + partition.partitionName() + "' contains invalid date format", e);
            }
        }
    }
}
