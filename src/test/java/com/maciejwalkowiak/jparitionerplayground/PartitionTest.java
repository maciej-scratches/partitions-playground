package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionTest {

    @Nested
    class Daily {

        @Test
        void resolvesParentTableName() {
            assertThat(Partition.of("events_20230112").parentTableName()).isEqualTo("events");
        }

        @Test
        void throwsWhenPartitionNameDoesNotHaveSeparator() {
            assertThatThrownBy(() -> Partition.of("20230112")).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void resolvesRange() {
            var add = Partition.of("events_20240105");
            assertThat(add.start()).isEqualTo(LocalDateTime.of(2024, 1, 5, 0, 0, 0));
            assertThat(add.end()).isEqualTo(LocalDateTime.of(2024, 1, 6, 0, 0, 0));
        }

        @Test
        void isValid() {
            var p = Partition.of("events_20230112");
            p.validate(PartitionConfig.forTable("events")
                    .rangeType(RangeType.DAILY));
        }

        @Test
        void isInvalidWhenTableNameDoesNotMatch() {
            var config = PartitionConfig.forTable("xxx")
                    .rangeType(RangeType.DAILY);
            var p = Partition.of("events_20230112");

            assertThatThrownBy(() -> p.validate(config)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void isInvalidWhenSuffixDoesNotMatch() {
            var config = PartitionConfig.forTable("events")
                    .rangeType(RangeType.DAILY);
            var p = Partition.of("events_202301");

            assertThatThrownBy(() -> p.validate(config)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Monthly {

        @Test
        void resolvesRange() {
            var add = Partition.of("events_202401");
            assertThat(add.start()).isEqualTo(LocalDateTime.of(2024, 1, 1, 0, 0, 0));
            assertThat(add.end()).isEqualTo(LocalDateTime.of(2024, 2, 1, 0, 0, 0));
        }

        @Test
        void isValid() {
            var p = Partition.of("events_202301");
            p.validate(PartitionConfig.forTable("events")
                    .rangeType(RangeType.MONTHLY));
        }

        @Test
        void isInvalidWhenTableNameDoesNotMatch() {
            var config = PartitionConfig.forTable("xxx")
                    .rangeType(RangeType.MONTHLY);
            var p = Partition.of("events_202301");

            assertThatThrownBy(() -> p.validate(config)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void isInvalidWhenSuffixDoesNotMatch() {
            var config = PartitionConfig.forTable("events")
                    .rangeType(RangeType.MONTHLY);
            var p = Partition.of("events_20230112");

            assertThatThrownBy(() -> p.validate(config)).isInstanceOf(IllegalStateException.class);
        }
    }

}