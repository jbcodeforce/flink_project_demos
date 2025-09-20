# Customer Analytics C360 Data Models

## Overview

This document defines the data models for the Customer 360 data product across four key business domains: 

- **Customer Domain**: CRM, loyalty program, support tickets, app usage
- **Sales Domain**: Transactions, line items, regional sales  
- **Product Domain**: Catalog, inventory, suppliers, relationships
- **Logistics Domain**: Shipments, warehouses, tracking events

## 1. Customer Domain

* **Data Sources**: CRM system, loyalty program, support tickets, app usage logs
* **Data samples**: c360_mock_data/customer

### Tables:

#### customers.csv
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Unique customer identifier |
| first_name | STRING | Customer first name |
| last_name | STRING | Customer last name |
| email | STRING | Customer email address |
| phone | STRING | Customer phone number |
| date_of_birth | DATE | Customer date of birth |
| gender | STRING | Customer gender (M/F/O) |
| registration_date | TIMESTAMP | When customer registered |
| customer_segment | STRING | Customer segment (Premium, Standard, Basic) |
| preferred_channel | STRING | Preferred shopping channel (online, store, mobile) |
| address_line1 | STRING | Primary address |
| city | STRING | City |
| state | STRING | State/Province |
| zip_code | STRING | ZIP/Postal code |
| country | STRING | Country |

#### loyalty_program.csv
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Foreign key to customers |
| loyalty_tier | STRING | Loyalty tier (Bronze, Silver, Gold, Platinum) |
| points_balance | INTEGER | Current loyalty points |
| points_earned_ytd | INTEGER | Points earned year-to-date |
| points_redeemed_ytd | INTEGER | Points redeemed year-to-date |
| tier_start_date | DATE | When current tier was achieved |
| lifetime_value | DECIMAL(10,2) | Customer lifetime value |

#### support_tickets.csv
| Column | Type | Description |
|--------|------|-------------|
| ticket_id | STRING | Unique ticket identifier |
| customer_id | STRING | Foreign key to customers |
| created_date | TIMESTAMP | When ticket was created |
| resolved_date | TIMESTAMP | When ticket was resolved |
| category | STRING | Issue category (billing, product, shipping, etc.) |
| priority | STRING | Priority level (low, medium, high, urgent) |
| status | STRING | Current status (open, in_progress, resolved, closed) |
| channel | STRING | Contact channel (phone, email, chat, store) |
| satisfaction_score | INTEGER | Customer satisfaction (1-5) |

#### app_usage.csv
| Column | Type | Description |
|--------|------|-------------|
| usage_id | STRING | Unique usage record identifier |
| customer_id | STRING | Foreign key to customers |
| session_date | DATE | Date of app usage |
| session_start | TIMESTAMP | Session start time |
| session_duration_minutes | INTEGER | Session duration in minutes |
| pages_viewed | INTEGER | Number of pages viewed |
| actions_taken | INTEGER | Number of actions taken |
| device_type | STRING | Device type (ios, android, web) |
| app_version | STRING | App version used |

## 2. Sales Domain

* **Data Sources**: Point of Sale (POS) system, e-commerce transactions, regional sales ledgers
* **Data samples**: c360_mock_data/sales

### Tables:

#### transactions.csv
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | Unique transaction identifier |
| customer_id | STRING | Foreign key to customers |
| transaction_date | TIMESTAMP | When transaction occurred |
| channel | STRING | Sales channel (store, online, mobile) |
| store_id | STRING | Store identifier (null for online) |
| payment_method | STRING | Payment method used |
| subtotal | DECIMAL(10,2) | Subtotal before tax/discounts |
| tax_amount | DECIMAL(10,2) | Tax amount |
| discount_amount | DECIMAL(10,2) | Discount applied |
| total_amount | DECIMAL(10,2) | Final transaction total |
| currency | STRING | Currency code |
| status | STRING | Transaction status (completed, cancelled, refunded) |

#### transaction_items.csv
| Column | Type | Description |
|--------|------|-------------|
| item_id | STRING | Unique line item identifier |
| transaction_id | STRING | Foreign key to transactions |
| product_id | STRING | Foreign key to products |
| quantity | INTEGER | Quantity purchased |
| unit_price | DECIMAL(10,2) | Price per unit |
| line_total | DECIMAL(10,2) | Total for this line item |
| discount_applied | DECIMAL(10,2) | Discount on this item |

#### regional_sales.csv
| Column | Type | Description |
|--------|------|-------------|
| region_id | STRING | Unique region identifier |
| region_name | STRING | Region name |
| country | STRING | Country |
| sales_date | DATE | Sales date |
| total_revenue | DECIMAL(12,2) | Total revenue for region/date |
| total_transactions | INTEGER | Number of transactions |
| average_order_value | DECIMAL(10,2) | Average order value |

## 3. Product Domain

* **Data Sources**: Product catalog, inventory system, supplier data
* **Data samples**: c360_mock_data/products

### Tables:

#### products.csv
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Unique product identifier |
| product_name | STRING | Product name |
| category | STRING | Product category |
| subcategory | STRING | Product subcategory |
| brand | STRING | Product brand |
| price | DECIMAL(10,2) | Current price |
| cost | DECIMAL(10,2) | Product cost |
| weight_kg | DECIMAL(8,3) | Product weight in kg |
| dimensions | STRING | Product dimensions |
| color | STRING | Product color |
| size | STRING | Product size |
| created_date | DATE | When product was created |
| status | STRING | Product status (active, discontinued, seasonal) |

#### inventory.csv
| Column | Type | Description |
|--------|------|-------------|
| inventory_id | STRING | Unique inventory record identifier |
| product_id | STRING | Foreign key to products |
| location_id | STRING | Warehouse or store identifier |
| stock_quantity | INTEGER | Current stock level |
| reserved_quantity | INTEGER | Reserved stock |
| reorder_point | INTEGER | Reorder threshold |
| max_stock_level | INTEGER | Maximum stock level |
| last_updated | TIMESTAMP | When record was last updated |

#### suppliers.csv
| Column | Type | Description |
|--------|------|-------------|
| supplier_id | STRING | Unique supplier identifier |
| supplier_name | STRING | Supplier company name |
| contact_person | STRING | Primary contact |
| email | STRING | Contact email |
| phone | STRING | Contact phone |
| address | STRING | Supplier address |
| country | STRING | Supplier country |
| payment_terms | STRING | Payment terms |
| quality_rating | DECIMAL(3,2) | Quality rating (1.00-5.00) |

#### product_suppliers.csv
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | Foreign key to products |
| supplier_id | STRING | Foreign key to suppliers |
| is_primary | BOOLEAN | Is primary supplier for this product |
| cost_price | DECIMAL(10,2) | Cost from this supplier |
| lead_time_days | INTEGER | Lead time in days |

## 4. Logistics Domain

* **Data Sources**: Warehouse management, shipping manifests, tracking data
* **Data samples**: c360_mock_data/logistic

### Tables:

#### shipments.csv
| Column | Type | Description |
|--------|------|-------------|
| shipment_id | STRING | Unique shipment identifier |
| transaction_id | STRING | Foreign key to transactions |
| tracking_number | STRING | Carrier tracking number |
| carrier | STRING | Shipping carrier |
| service_level | STRING | Service level (standard, expedited, overnight) |
| origin_location | STRING | Origin warehouse/store |
| destination_address | STRING | Destination address |
| weight_kg | DECIMAL(8,3) | Shipment weight |
| dimensions | STRING | Package dimensions |
| ship_date | DATE | Date shipped |
| estimated_delivery | DATE | Estimated delivery date |
| actual_delivery | DATE | Actual delivery date |
| delivery_status | STRING | Current delivery status |
| shipping_cost | DECIMAL(8,2) | Shipping cost |

#### warehouse_locations.csv
| Column | Type | Description |
|--------|------|-------------|
| location_id | STRING | Unique location identifier |
| location_name | STRING | Location name |
| location_type | STRING | Type (warehouse, distribution_center, store) |
| address | STRING | Full address |
| city | STRING | City |
| state | STRING | State/Province |
| country | STRING | Country |
| capacity_cubic_meters | INTEGER | Storage capacity |
| manager_name | STRING | Location manager |

#### tracking_events.csv
| Column | Type | Description |
|--------|------|-------------|
| event_id | STRING | Unique event identifier |
| shipment_id | STRING | Foreign key to shipments |
| event_timestamp | TIMESTAMP | When event occurred |
| event_type | STRING | Event type (picked_up, in_transit, out_for_delivery, delivered, etc.) |
| location | STRING | Event location |
| description | STRING | Event description |
| carrier_status | STRING | Carrier's status code |

## Data Relationships

### Key Relationships:

- `customers.customer_id` → Primary key for customer dimension
- `transactions.customer_id` → Links sales to customers
- `transaction_items.product_id` → Links sales to products
- `shipments.transaction_id` → Links logistics to sales
- `inventory.product_id` → Links inventory to products
- `product_suppliers.product_id` → Links suppliers to products

### C360 Integration Points:

- **Customer Profile**: Combine customers, loyalty_program, support_tickets, app_usage
- **Purchase History**: Join transactions with transaction_items and products  
- **Fulfillment Data**: Link shipments and tracking_events to customer orders
- **Product Preferences**: Analyze transaction_items to understand product affinity

## Mock Data Generation

- **Location**: `c360_mock_data/` directory
- **Volume**: 15 CSV files with realistic mock data
- **Quality**: Referential integrity maintained across domains
- **Structure**:
  ```
  c360_mock_data/
  ├── customer/     (4 files: customers, loyalty, support, app usage)
  ├── sales/        (3 files: transactions, items, regional data)
  ├── products/     (4 files: products, inventory, suppliers, relationships)  
  └── logistics/    (3 files: shipments, locations, tracking)
  ```