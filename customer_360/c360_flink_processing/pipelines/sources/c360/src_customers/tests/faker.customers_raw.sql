create table customers_raw (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    gender STRING,
    registration_date TIMESTAMP(3),
    customer_segment STRING,
    preferred_channel STRING,
    address_line1 STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING,
    event_ts TIMESTAMP(3)
) distributed by hash(customer_id) into 1 buckets with (


    'connector' = 'faker',
    'number-of-rows' = '500',
    'changelog.mode' = 'append',
    'fields.customer_id.expression' = '#{numerify ''CUST###''}',
    'fields.first_name.expression' = '#{Name.firstName}',
    'fields.last_name.expression' = '#{Name.lastName}',
    'fields.email.expression' = '#{Internet.emailAddress}',
    'fields.phone.expression' = '#{PhoneNumber.cellPhone}',
    'fields.date_of_birth.expression' = '#{date.birthday ''18'',''50''}',
    'fields.gender.expression' = '#{Options.option ''Male'',''Female''}',
    'fields.registration_date.expression' = '#{date.past ''30'', ''DAYS''}',
    'fields.customer_segment.expression' = '#{Options.option ''Gold'',''Silver'',''Bronze''}',
    'fields.preferred_channel.expression' = '#{Options.option ''Email'',''SMS'',''Push''}',
    'fields.address_line1.expression' = '#{Address.streetAddress}',
    'fields.city.expression' = '#{Address.city}',
    'fields.state.expression' = '#{Address.state}',
    'fields.zip_code.expression' = '#{Address.zipCode}',
    'fields.country.expression' = '#{Address.country}'
)