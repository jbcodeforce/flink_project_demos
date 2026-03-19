with expected_results as (
    select 
    'customer_id_1' as expected_customer_id,    
    'first_name_1' as expected_first_name,    
    'last_name_1' as expected_last_name,    
    'email_1' as expected_email,    
    'customer_segment_1' as expected_customer_segment,    
    'preferred_channel_1' as expected_preferred_channel,    
    'generation_segment_1' as expected_generation_segment,    
    'age_years_1' as expected_age_years,    
    'days_since_registration_1' as expected_days_since_registration,    
    'city' as expected_city,    
    'state_1' as expected_state,    
    'country_1' as expected_country,    
    'loyalty_tier_1' as expected_loyalty_tier,    
    'points_balance' as expected_points_balance,    
    'lifetime_value' as expected_lifetime_value    
    
        
    -- union all -- add more union here for each potential test data
    
),
actual_results as (
    select 
        customer_id,
        first_name,
        last_name,
        email,
        customer_segment,
        preferred_channel,
        generation_segment,
        age_years,
        days_since_registration,
        city,
        state,
        country,
        loyalty_tier,
        points_balance,
        lifetime_value
        
    from c360_fct_customer_profile_ut
),
validation_check as (
    select 
       
        e.expected_customer_id,
        e.expected_first_name,
        e.expected_last_name,
        e.expected_email,
        e.expected_customer_segment,
        e.expected_preferred_channel,
        e.expected_generation_segment,
        e.expected_age_years,
        e.expected_days_since_registration,
        e.expected_city,
        e.expected_state,
        e.expected_country,
        e.expected_loyalty_tier,
        e.expected_points_balance,
        e.expected_lifetime_value,
        
        -- be sure to use the correct conditions for the check
        case when a.customer_id = e.expected_customer_id then 'PASS' else 'FAIL' end as customer_id_check,
        case when a.first_name = e.expected_first_name then 'PASS' else 'FAIL' end as first_name_check,
        case when a.last_name = e.expected_last_name then 'PASS' else 'FAIL' end as last_name_check,
        case when a.email = e.expected_email then 'PASS' else 'FAIL' end as email_check,
        case when a.customer_segment = e.expected_customer_segment then 'PASS' else 'FAIL' end as customer_segment_check,
        case when a.preferred_channel = e.expected_preferred_channel then 'PASS' else 'FAIL' end as preferred_channel_check,
        case when a.generation_segment = e.expected_generation_segment then 'PASS' else 'FAIL' end as generation_segment_check,
        case when a.age_years = e.expected_age_years then 'PASS' else 'FAIL' end as age_years_check,
        case when a.days_since_registration = e.expected_days_since_registration then 'PASS' else 'FAIL' end as days_since_registration_check,
        case when a.city = e.expected_city then 'PASS' else 'FAIL' end as city_check,
        case when a.state = e.expected_state then 'PASS' else 'FAIL' end as state_check,
        case when a.country = e.expected_country then 'PASS' else 'FAIL' end as country_check,
        case when a.loyalty_tier = e.expected_loyalty_tier then 'PASS' else 'FAIL' end as loyalty_tier_check,
        case when a.points_balance = e.expected_points_balance then 'PASS' else 'FAIL' end as points_balance_check,
        case when a.lifetime_value = e.expected_lifetime_value then 'PASS' else 'FAIL' end as lifetime_value_check
        

    from expected_results e
    left join actual_results a on a.sid = e.sid -- !!! change the condition here
),
overall_result as (
    select 
        count(*) as total_expected_records,
        sum(case when customer_id_check = 'PASS' AND first_name_check = 'PASS' AND last_name_check = 'PASS' AND email_check = 'PASS' AND customer_segment_check = 'PASS' AND preferred_channel_check = 'PASS' AND generation_segment_check = 'PASS' AND age_years_check = 'PASS' AND days_since_registration_check = 'PASS' AND city_check = 'PASS' AND state_check = 'PASS' AND country_check = 'PASS' AND loyalty_tier_check = 'PASS' AND points_balance_check = 'PASS' AND lifetime_value_check = 'PASS' then 1 else 0 end) as passing_records,
        (select count(*) from actual_results) as actual_record_count
    from validation_check
)
select 
    case 
        when total_expected_records = 1  -- should match the number of union
         and passing_records = 1
        then 'PASS' 
        else 'FAIL' 
    end as test_result,
    total_expected_records,
    passing_records
from overall_result