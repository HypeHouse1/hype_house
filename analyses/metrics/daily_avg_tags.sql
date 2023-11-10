with

videos_analytics as (

    select * from {{ ref('fct_videos_analytics') }}

),

date_tags_count as (

    select

        published_date
        , array_size(split(tags, '|')) as tags_count
    
    from videos_analytics

),

final as (

    select

        published_date
        , avg(tags_count) as avg_tags_daily

    from date_tags_count
    group by published_date
)

select * from final