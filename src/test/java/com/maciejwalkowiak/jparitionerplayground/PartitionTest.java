package com.maciejwalkowiak.jparitionerplayground;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PartitionTest {

    @Test
    void partition() {
        System.out.println(Partition.forTable("events")
                .byRange("created_at")
                .daily()
                .retention(7)
                .buffer(7)
                .toSql());
        System.out.println("\n");
        System.out.println(Partition.forTable("events")
                .byRange("created_at")
                .monthly()
                .retention(7)
                .buffer(7)
                .toSql());
    }

}