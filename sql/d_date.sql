with raw_dates as (
    select 
        explode(
            sequence( to_date('2000-01-01'), to_date('2099-12-31'), interval 1 day )
        ) as calendar_date
)

select 
    CAST(date_format(calendar_date, "yyyyMMdd") AS INT) as date_key,
    calendar_date as date_alternate_key,
    year(calendar_date) AS calendar_year,
    date_format(calendar_date, 'MMMM') as calendar_month,
    month(calendar_date) as month_of_year,
    date_format(calendar_date, 'EEEE') as calendar_day,
    dayofweek(calendar_date) AS day_of_week_sun,
    weekday(calendar_date) + 1 as day_of_week_mon,
    case
        when weekday(calendar_date) < 5 then 'Y'
        else 'N'
    end as is_week_day,
    dayofmonth(calendar_date) as day_of_month,
    case
        when calendar_date = last_day(calendar_date) then 'Y'
        else 'N'
    end as is_last_day_of_month,
    dayofyear(calendar_date) as day_of_year,
    weekofyear(calendar_date) as week_of_year_iso,
    quarter(calendar_date) as quarter_of_year
from 
    raw_dates