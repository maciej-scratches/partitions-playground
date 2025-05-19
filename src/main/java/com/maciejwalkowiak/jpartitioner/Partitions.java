package com.maciejwalkowiak.jpartitioner;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Manages creating and dropping partitions according to {@link PartitionConfig}.
 *
 * @author Maciej Walkowiak
 */
@Component
public class Partitions {
    private final PartitionRepository partitionRepository;

    public Partitions(PartitionRepository partitionRepository) {
        this.partitionRepository = partitionRepository;
    }

    /**
     * Refreshes partitions in the database according to a config for a current date.
     *
     * @param config - partitions config
     */
    public void refresh(PartitionConfig config) {
        refresh(LocalDate.now(), config);
    }

    /**
     * Refreshes partitions in the database according to a config for a given date.
     *
     * @param date - point in time as a reference to partition config
     * @param config - partition config
     */
    public void refresh(LocalDate date, PartitionConfig config) {
        PartitionChangeset changeset = diff(date, config);
        if (config.retentionPolicy() == RetentionPolicy.DETACH) {
            partitionRepository.detachPartitions(changeset.remove());
        } else if (config.retentionPolicy() == RetentionPolicy.DROP) {
            partitionRepository.dropPartitions(changeset.remove());
        };
        partitionRepository.createPartitions(changeset.add());
    }

    private PartitionChangeset diff(LocalDate date, PartitionConfig config) {
        List<Partition> existingPartitions = partitionRepository.findPartitions(config.tableName());
        existingPartitions.forEach(it -> it.validate(config));

        List<Partition> expectedPartitions = this.expectedPartitions(config, date);

        List<Partition> partitionsToDrop = existingPartitions
                .stream()
                .filter(it -> !expectedPartitions.contains(it))
                .toList();

        List<Partition> partitionsToAdd = expectedPartitions.stream()
                .filter(it -> !existingPartitions.contains(it))
                .toList();
        return new PartitionChangeset(partitionsToDrop, partitionsToAdd);
    }

    private List<Partition> expectedPartitions(PartitionConfig config, LocalDate date) {
        return IntStream.range(-config.retention(), config.buffer())
                .mapToObj(i -> Partition.of(config.tableName(), config.rangeType(), partitionDate(config, date, i)))
                .toList();
    }

    private LocalDate partitionDate(PartitionConfig config, LocalDate date, int i) {
        return switch (config.rangeType()) {
            case DAILY -> date.plusDays(i);
            case MONTHLY -> date.plusMonths(i);
        };
    }

    private record PartitionChangeset(List<Partition> remove, List<Partition> add) {
    }
}
