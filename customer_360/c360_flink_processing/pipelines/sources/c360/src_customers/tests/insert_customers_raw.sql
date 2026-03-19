INSERT INTO customers_raw (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    state,
    zip_code,
    country,
    event_ts
)
VALUES
    ('TEST001', 'Sarah', 'Johnson', 'sarah.johnson@email.com', '+1-555-0101', DATE '1985-03-15', 'F', TIMESTAMP '2020-01-15 10:30:00', 'Premium', 'online', '123 Main St', 'Seattle', 'WA', '98101', 'USA', TIMESTAMP '2026-01-28 10:00:00'),
    ('TEST002', 'Michael', 'Chen', 'michael.chen@email.com', '+1-555-0102', DATE '1990-07-22', 'M', TIMESTAMP '2019-05-20 14:15:00', 'Standard', 'store', '456 Oak Ave', 'Portland', 'OR', '97201', 'USA', TIMESTAMP '2026-01-28 10:00:01'),
    ('TEST003', 'Emily', 'Rodriguez', 'emily.rodriguez@email.com', '+1-555-0103', DATE '1988-12-03', 'F', TIMESTAMP '2021-03-10 09:45:00', 'Premium', 'mobile', '789 Pine St', 'San Francisco', 'CA', '94102', 'USA', TIMESTAMP '2026-01-28 10:00:02'),
    ('TEST004', 'David', 'Thompson', 'david.thompson@email.com', '+1-555-0104', DATE '1975-11-18', 'M', TIMESTAMP '2018-08-05 16:20:00', 'Basic', 'online', '321 Elm Dr', 'Los Angeles', 'CA', '90210', 'USA', TIMESTAMP '2026-01-28 10:00:03');