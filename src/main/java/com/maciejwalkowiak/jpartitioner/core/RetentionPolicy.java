package com.maciejwalkowiak.jpartitioner.core;

/**
 * Defines what to do with partitions older than configured retention.
 *
 * @author Maciej Walkowiak
 */
public enum RetentionPolicy {
    /**
     * Detaches partition from the main table, but does not drop any data.
     */
    DETACH,

    /**
     * Detaches and drops partition - this is a destructive operation.
     */
    DROP
}
