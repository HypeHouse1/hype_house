with 

channels_multiple_trending_videos as (

    select * from {{ ref('int_channels_multiple_trending_videos') }}

)

, channels_trending_consecutive_days as (

    select 

        channel_id
        , video_id
    
    from channels_multiple_trending_videos

    group by 

        channel_id
        , video_id
        , partition_id

    having count(*) > 1

)

, channels_trending_consecutive_days_no_gaps as (

    select

        channel_id
        , video_id

    from channels_trending_consecutive_days
    
    group by 

        channel_id
        , video_id
    
    having count(*) = 1

)

, final as (

    select

        channels_multiple_trending_videos.channel_id
        , channels_multiple_trending_videos.video_id
        , channels_multiple_trending_videos.trending_date 
        
        , channels_multiple_trending_videos.tags
        , channels_multiple_trending_videos.likes_count
        , channels_multiple_trending_videos.comment_count
        , channels_multiple_trending_videos.view_count
               
    from channels_trending_consecutive_days_no_gaps
    
    left join channels_multiple_trending_videos
        on channels_trending_consecutive_days_no_gaps.video_id = channels_multiple_trending_videos.video_id
)

select * from final
