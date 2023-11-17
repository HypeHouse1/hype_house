with

video_analytics as (

    select * from {{ ref('video_analytics') }}

),

distinct_videos as (

    select distinct

        channel_id
        , channel_title
        , video_id
    
    from video_analytics

),

final as (

    select

        channel_id
        , channel_title
        , count(*) as trending_videos_count

    from distinct_videos
    group by
        channel_id
        , channel_title

)

select * from final