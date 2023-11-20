with 

video_analytics as (

    select * from {{ ref('video_analytics') }}

),

video_dates as (

    select

        video_id
        , published_date
        , min(trending_date) as trending_date

    from video_analytics
    group by

        video_id
        , published_date

),

final as (

    select 

        round(avg(datediff(day, published_date, trending_date)), 2) as avg_days_to_trend

    from video_dates

)

select * from final