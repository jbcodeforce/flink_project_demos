set 'client.statement-name' = 'insert-support-ticket-raw';
create table support_ticket_raw (
    ticket_id STRING,
    customer_id STRING,
    created_date TIMESTAMP(3),
    resolved_date TIMESTAMP(3),
    category STRING,
    priority STRING,
    status STRING,
    channel STRING,
    satisfaction_score INTEGER
) distributed by hash(ticket_id) into 1 buckets with (
    'changelog.mode' = 'append',
    'connector' = 'faker',
    'number-of-rows' = '15000',
    'rows-per-second' = '5',
    'fields.ticket_id.expression' = '#{IdNumber.valid}',
    'fields.customer_id.expression' = '#{numerify ''CUST###''}',
    'fields.created_date.expression' = '#{date.past ''30'', ''DAYS''}',
    'fields.resolved_date.expression' = '#{date.past ''30'', ''DAYS''}',
    'fields.category.expression' = '#{Options.option ''Account'',''Billing'',''Technical''}',
    'fields.priority.expression' = '#{Options.option ''Low'',''Medium'',''High''}',
    'fields.status.expression' = '#{Options.option ''Open'',''Closed''}',
    'fields.channel.expression' = '#{Options.option ''Email'',''Phone'',''Chat''}'
)