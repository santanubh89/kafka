package com.kafka.producer;

import lombok.ToString;

public record EmployeeData (
        String id,
        String name,
        Department department,
        double salary,
        Address address
){}

record Address (
        String city,
        String state,
        String country
) {}

enum Department {
    HR, IT, SALES, MARKETING
}