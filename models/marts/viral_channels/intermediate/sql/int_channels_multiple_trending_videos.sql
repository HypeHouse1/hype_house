with

video_analytics as (

    select * from {{ ref('video_analytics') }}

),

channel_videos as (

    select

        channel_id
        , channel_title
        , video_id
    
    from video_analytics

    group by 

        channel_id
        , channel_title
        , video_id

),

channels_multiple_trending_videos as (

    select

        channel_id
        , channel_title
    
    from channel_videos

    group by 

        channel_id
        , channel_title
    
    having count(*) > 1

),

final as (

    select

        video_analytics.channel_id
        , video_analytics.channel_title
        , video_analytics.video_id
        , video_analytics.trending_date
        , video_analytics.likes_count
        , video_analytics.comment_count
        , video_analytics.view_count
        
        , datediff(day, '2023-01-01', video_analytics.trending_date) - row_number() over (
            partition by video_id 
            order by trending_date asc
        ) as partition_id 

    from channels_multiple_trending_videos

    left join video_analytics
        on channels_multiple_trending_videos.channel_id = video_analytics.channel_id

)

select * from final