with 

channels_trending_consecutive_days as (

    select * from {{ ref('int_channels_trending_consecutive_days') }}

)

, channels_trending_videos as (

    select

        channel_id
        , video_id

        , count(trending_date) as trending_days

        , avg(array_size(tags)) as avg_tags_count
        , avg(likes_count) as avg_likes_count
        , avg(comment_count) as avg_comment_count
        , avg(view_count) as avg_view_count

    from channels_trending_consecutive_days

    group by 

        channel_id
        , video_id

)

, final as (

    select

        channel_id

        , count(video_id) as consecutive_trending_video_count
        , round(avg(trending_days), 2) as avg_trending_days

        , round(avg(avg_tags_count), 2) as avg_tags_count
      
        , round(avg(avg_likes_count), 2) as avg_likes_count
        , round(avg(avg_comment_count), 2) as avg_comment_count
        , round(avg(avg_view_count), 2) as avg_view_count
    
    from channels_trending_videos

    group by 

        channel_id

)

select * from final