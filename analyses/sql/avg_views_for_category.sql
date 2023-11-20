with 

video_analytics as (

    select * from {{ ref('video_analytics') }}

),

video_trending_date as (

    select

        video_id
        , max(trending_date) as trending_date

    from video_analytics
    group by

        video_id

),

final as (

    select

        category_id
        , category_name
        , round(avg(view_count), 2) as avg_views

    from video_trending_date
    left join video_analytics using(video_id, trending_date)
    group by

        category_id
        , category_name

)

select * from final