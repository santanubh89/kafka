package com.kafka.consumer;

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