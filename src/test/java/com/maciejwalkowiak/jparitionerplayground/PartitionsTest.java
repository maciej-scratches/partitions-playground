package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PartitionsTest {
    private final PartitionRepository partitionRepository = mock();
    private final Partitions partitions = new Partitions(partitionRepository);

    @Nested
    class Daily {

        @Test
        void refreshesPartitions() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(new PartitionName("events_20241228"), new PartitionName("events_20241227")));

            partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(PartitionConfig.RangeType.DAILY)
                    .buffer(3)
                    .retention(3));

            verify(partitionRepository).detachPartitions(eq("events"), assertArg(it -> {
                assertThat(it).containsExactly(new DropPartition("events_20241228"), new DropPartition("events_20241227"));
            }));

            verify(partitionRepository).createPartitions(eq("events"), assertArg(it -> {
                assertThat(it).containsExactlyInAnyOrder(
                        new AddPartition("20250101", LocalDateTime.of(2025, 1, 1, 0, 0, 0), LocalDateTime.of(2025, 1, 1, 23, 59, 59)),
                        new AddPartition("20250102", LocalDateTime.of(2025, 1, 2, 0, 0, 0), LocalDateTime.of(2025, 1, 2, 23, 59, 59)),
                        new AddPartition("20250103", LocalDateTime.of(2025, 1, 3, 0, 0, 0), LocalDateTime.of(2025, 1, 3, 23, 59, 59)),
                        new AddPartition("20250104", LocalDateTime.of(2025, 1, 4, 0, 0, 0), LocalDateTime.of(2025, 1, 4, 23, 59, 59)),
                        new AddPartition("20241231", LocalDateTime.of(2024, 12, 31, 0, 0, 0), LocalDateTime.of(2024, 12, 31, 23, 59, 59)),
                        new AddPartition("20241230", LocalDateTime.of(2024, 12, 30, 0, 0, 0), LocalDateTime.of(2024, 12, 30, 23, 59, 59))
                );
            }));
        }

        @Test
        void failsWhenInvalidPartitionNameExists() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(new PartitionName("events_202412"), new PartitionName("events_20241227")));

            assertThatThrownBy(() -> partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(PartitionConfig.RangeType.DAILY)
                    .buffer(3)
                    .retention(3))).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Monthly {

        @Test
        void failsWhenInvalidPartitionNameExists() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(new PartitionName("events_202412"), new PartitionName("events_20241227")));

            assertThatThrownBy(() -> partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(PartitionConfig.RangeType.DAILY)
                    .buffer(3)
                    .retention(3))).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void refreshesPartitions() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(new PartitionName("events_202412"), new PartitionName("events_202411"), new PartitionName("events_202410")));

            partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(PartitionConfig.RangeType.MONTHLY)
                    .buffer(3)
                    .retention(2));

            verify(partitionRepository).detachPartitions(eq("events"), assertArg(it -> {
                new DropPartition("events_202410");
            }));

            verify(partitionRepository).createPartitions(eq("events"), assertArg(it -> {
                assertThat(it).containsExactlyInAnyOrder(
                        new AddPartition("202501", LocalDateTime.of(2025, 1, 1, 0, 0, 0), LocalDateTime.of(2025, 1, 31, 23, 59, 59)),
                        new AddPartition("202502", LocalDateTime.of(2025, 2, 1, 0, 0, 0), LocalDateTime.of(2025, 2, 28, 23, 59, 59)),
                        new AddPartition("202503", LocalDateTime.of(2025, 3, 1, 0, 0, 0), LocalDateTime.of(2025, 3, 31, 23, 59, 59))
                );
            }));
        }
    }
}