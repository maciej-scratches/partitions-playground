package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PartitionsTest {
    private final PartitionRepository partitionRepository = mock();
    private final Partitions partitions = new Partitions(partitionRepository);

    @Nested
    class Daily {

        @ParameterizedTest
        @EnumSource(RetentionPolicy.class)
        void refreshesPartitions(RetentionPolicy retentionPolicy) {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(Partition.of("events_20241228"), Partition.of("events_20241227")));

            partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(RangeType.DAILY)
                    .buffer(3)
                    .retention(3, retentionPolicy));

            if (retentionPolicy == RetentionPolicy.DETACH) {
                verify(partitionRepository).detachPartitions(assertArg(it -> {
                    assertThat(it).containsExactly(Partition.of("events_20241228"), Partition.of("events_20241227"));
                }));
            } else if (retentionPolicy == RetentionPolicy.DROP) {
                verify(partitionRepository).dropPartitions(assertArg(it -> {
                    assertThat(it).containsExactly(Partition.of("events_20241228"), Partition.of("events_20241227"));
                }));
            } else {
                fail("Unexpected retention policy: " + retentionPolicy);
            }

            verify(partitionRepository).createPartitions(assertArg(it -> {
                assertThat(it).containsExactlyInAnyOrder(
                        Partition.of("events_20250101"),
                        Partition.of("events_20250102"),
                        Partition.of("events_20250103"),
                        Partition.of("events_20250104"),
                        Partition.of("events_20241231"),
                        Partition.of("events_20241230")
                );
            }));
        }

        @Test
        void failsWhenInvalidPartitionNameExists() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(Partition.of("events_202412"), Partition.of("events_20241227")));

            assertThatThrownBy(() -> partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(RangeType.DAILY)
                    .buffer(3)
                    .retention(3, RetentionPolicy.DETACH))).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Monthly {

        @Test
        void failsWhenInvalidPartitionNameExists() {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(Partition.of("events_202412"), Partition.of("events_20241227")));

            assertThatThrownBy(() -> partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(RangeType.DAILY)
                    .buffer(3)
                    .retention(3, RetentionPolicy.DETACH))).isInstanceOf(IllegalStateException.class);
        }

        @ParameterizedTest
        @EnumSource(RetentionPolicy.class)
        void refreshesPartitions(RetentionPolicy retentionPolicy) {
            when(partitionRepository.findPartitions("events")).thenReturn(List.of(Partition.of("events_202412"), Partition.of("events_202411"), Partition.of("events_202410")));

            partitions.refresh(LocalDate.of(2025, 1, 2), PartitionConfig.forTable("events")
                    .rangeType(RangeType.MONTHLY)
                    .buffer(3)
                    .retention(2, retentionPolicy));

            if (retentionPolicy == RetentionPolicy.DETACH) {
                verify(partitionRepository).detachPartitions(List.of(Partition.of("events_202410")));
            } else if (retentionPolicy == RetentionPolicy.DROP) {
                verify(partitionRepository).dropPartitions(List.of(Partition.of("events_202410")));
            } else {
                fail("Unexpected retention policy: " + retentionPolicy);
            }

            verify(partitionRepository).createPartitions(assertArg(it -> {
                assertThat(it).containsExactlyInAnyOrder(
                        Partition.of("events_202501"),
                        Partition.of("events_202502"),
                        Partition.of("events_202503")
                );
            }));
        }
    }
}