with

videos_analytics as (

    select * from {{ ref('fct_videos_analytics') }}

),

published_date_tags_count as (

    select

        published_date
        , array_size(split(tags, '|')) as tags_count
    
    from videos_analytics

),

final as (

    select

        published_date
        , round(avg(tags_count), 2) as avg_tags_daily

    from published_date_tags_count
    group by published_date
)

select * from final