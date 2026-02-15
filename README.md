# Midas
# JPMorgan Midas Core – Transaction Processing Microservice

Backend microservice built as part of the JPMorgan Chase Advanced Software Engineering Virtual Experience (Forage).

##  Overview

Midas Core is a Spring Boot microservice that processes financial transactions using Apache Kafka, persists data using H2 database with JPA, integrates an external REST Incentive API, and exposes a REST endpoint to retrieve user balances.

##  Tech Stack

- Java 17
- Spring Boot
- Spring Data JPA
- Apache Kafka
- H2 In-Memory Database
- REST APIs
- Maven

##  Architecture

- Kafka Listener for transaction ingestion
- Service layer for business logic
- Repository layer using JPA
- REST Controller for balance retrieval
- H2 Database for persistence

##  Features Implemented

- Integrated Kafka consumer for transaction processing
- Persisted transaction records using Spring Data JPA
- Connected to external REST Incentive API
- Updated user balances dynamically
- Exposed REST endpoint: `GET /balance?userId=1`

## Key Learning

- Event-driven architecture
- Backend microservice design
- API integration
- Database modeling with JPA
- Clean layered architecture

##  Certification

JPMorgan Chase Software Engineering Job Simulation – February 2026

