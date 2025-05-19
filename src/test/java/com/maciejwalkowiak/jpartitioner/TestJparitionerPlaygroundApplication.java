package com.maciejwalkowiak.jpartitioner;

import org.springframework.boot.SpringApplication;

public class TestJparitionerPlaygroundApplication {

    public static void main(String[] args) {
        SpringApplication.from(JparitionerPlaygroundApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
