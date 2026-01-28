# Agent Description for Flink Project Demos Repository

## Repository Overview
This repository demonstrates the journey from batch to real-time data processing across multiple use cases. It showcases both Apache Spark batch processing and Apache Flink stream processing implementations, with emphasis on data as a product methodology and shift-left practices. Primary use cases include Customer 360 (C360) analytics and transaction processing with fraud detection patterns.

## Repository Structure & Agent Responsibilities

### `/c360_api` - REST API Service
- **Purpose**: Provides REST API endpoints for accessing C360 data
- **Agent Role**: 
  - Maintain FastAPI-based service implementation
  - Ensure proper database connectivity and models
  - Keep API documentation current
  - Manage test coverage and performance

### `/c360_spark_processing` - Batch Processing Implementation
- **Purpose**: Contains the original batch processing pipeline using Apache Spark
- **Agent Role**:
  - Maintain SQL-based dimensional modeling
  - Ensure proper data transformation logic
  - Keep pipeline scripts updated and documented
  - Validate data quality and consistency

#### Key Components:
- **Dimensions**: Customer, product, and location dimension tables
- **Facts**: Sales and logistics fact tables
- **Views**: Business-specific analytical views
- **Pipeline Scripts**: End-to-end batch processing orchestration

### `/c360_flink_processing` - Stream Processing Implementation
- **Purpose**: Contains the real-time processing pipeline using Apache Flink
- **Agent Role**:
  - Maintain Flink SQL streaming queries
  - Ensure proper state management and checkpointing
  - Keep deployment configurations updated
  - Document streaming patterns and best practices

#### Key Components:
- **Pipelines**: Flink SQL job definitions and configurations
- **Documentation**: Stream processing patterns and deployment guides
- **Staging**: Development and testing configurations

### `/c360_mock_data` - Test Data Generation
- **Purpose**: Provides sample datasets for development and testing
- **Agent Role**:
  - Maintain realistic test data generation
  - Ensure data consistency across domains
  - Keep data schemas current
  - Document data models and relationships

#### Data Domains:
- **Customer**: Customer profile and preference data
- **Products**: Product catalog and inventory data
- **Sales**: Transaction and order data
- **Logistics**: Shipping and delivery data

### `/tx_processing` - Transaction Processing Application
- **Purpose**: Demonstrates real-time transaction processing and fraud detection patterns using Apache Flink
- **Agent Role**:
  - Maintain Flink SQL streaming pipelines for transaction processing
  - Ensure proper dimensional modeling for transaction analytics
  - Manage Terraform infrastructure as code for deployment
  - Document stream processing patterns and business logic
  - Support shift_left project management workflow

#### Key Components:
- **Pipelines**: Flink SQL definitions for sources, dimensions, and facts
  - Source tables: customers, transactions, discounts
  - Dimension tables: flagged transactions, customer analytics
  - Fact tables: transaction aggregations and analytics
- **Terraform**: Infrastructure as code for Confluent Cloud deployment
- **Documentation**: Domain model, relationships, and deployment guides
- **Staging**: Development and testing configurations
- **Tests**: Faker-based data generators for synthetic test data

#### Domain Characteristics:
- **Source Tables**: Customer master data, financial transactions, merchant discounts
- **Processing Patterns**: Upsert mode for deduplication, append-only for events
- **Watermarks**: Timestamp-based event time processing
- **Use Cases**: Transaction flagging, customer analytics, discount application

### `/docs` - Documentation (MkDocs Configuration)
- **Purpose**: Contains all documentation files configured for MkDocs
- **Agent Role**: 
  - Maintain technical documentation
  - Document migration patterns and best practices
  - Keep architecture diagrams current
  - Manage deployment guides

## Agent Capabilities & Responsibilities

### Core Competencies Required:
1. **Data Processing Expertise**
   - Apache Spark SQL and batch processing
   - Apache Flink SQL and stream processing
   - Data modeling and transformation patterns
   - Performance optimization techniques

2. **Infrastructure Knowledge**
   - Confluent Cloud for Flink
   - Apache Kafka integration
   - Terraform infrastructure as code
   - REST API development
   - Container deployment

3. **Documentation Management**
   - MkDocs configuration
   - Technical writing
   - Architecture documentation
   - API documentation

4. **Development Support**
   - Python programming (FastAPI)
   - SQL optimization (Spark and Flink)
   - Testing and validation
   - Data quality management

### Primary Agent Tasks:
- **Migration Support**: Guide batch to streaming migration
- **Code Maintenance**: Keep processing pipelines current
- **Documentation**: Maintain clear migration guides
- **Testing**: Ensure data consistency across implementations
- **Performance**: Optimize processing efficiency
- **Integration**: Maintain smooth API and processing pipeline integration

### Agent Interaction Guidelines:
- Focus on practical migration patterns from batch to streaming
- Maintain clear documentation of architectural decisions
- Ensure proper testing across both processing paradigms
- Keep deployment configurations environment-agnostic
- Document performance implications of processing choices

## Success Metrics:
- Both batch and streaming pipelines function correctly
- API endpoints provide consistent data access
- Documentation clearly explains migration patterns
- Test data accurately represents production scenarios
- Performance metrics are documented and optimized
- Deployment guides are clear and reproducible
- Terraform deployments are consistent and repeatable
- Transaction processing pipelines handle real-time event streams effectively