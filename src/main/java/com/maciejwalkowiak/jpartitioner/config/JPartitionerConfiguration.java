package com.maciejwalkowiak.jpartitioner.config;

import com.maciejwalkowiak.jpartitioner.core.JdbcPartitionRepository;
import com.maciejwalkowiak.jpartitioner.core.Partitions;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
public class JPartitionerConfiguration {

    @Bean
    JdbcPartitionRepository jdbcPartitionRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcPartitionRepository(jdbcTemplate);
    }

    @Bean
    Partitions partitions(JdbcPartitionRepository jdbcPartitionRepository) {
        return new Partitions(jdbcPartitionRepository);
    }
}
