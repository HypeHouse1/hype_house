with 

channels_multiple_trending_videos as (

    select * from {{ ref('int_channels_multiple_trending_videos') }}

)

, channels_multiple_trending_consecutive_days as (

    select 

        channel_id
        , channel_title
        , video_id
        , min(trending_date) as start_trending_date
        
        , array_agg(trending_date) within group (order by trending_date asc) as trending_dates

        , avg(likes_count) as avg_likes_count
        , avg(comment_count) as avg_comment_count
        , avg(view_count) as avg_view_count
    
    from channels_multiple_trending_videos

    group by 

        channel_id
        , channel_title
        , video_id
        , partition_id

    having count(*) > 1

)

, channels_multiple_trending_consecutive_days_no_gaps as (

    select

        channel_id
        , channel_title
        , video_id

        , array_union_agg(trending_dates) as trending_dates

        -- these sums are here just for make the columns compatible with the group by
        , sum(avg_likes_count) as avg_likes_count
        , sum(avg_comment_count) as avg_comment_count
        , sum(avg_view_count) as avg_view_count

    from channels_multiple_trending_consecutive_days
    
    group by 

        channel_id
        , channel_title
        , video_id
    
    having count(*) = 1

)

, final as (

    select

        channel_id
        , channel_title

        , count(video_id) as consecutive_trending_video_count
      
        , round(avg(avg_likes_count), 2) as avg_likes_count
        , round(avg(avg_comment_count), 2) as avg_comment_count
        , round(avg(avg_view_count), 2) as avg_view_count
    
    from channels_multiple_trending_consecutive_days_no_gaps

    group by 

        channel_id
        , channel_title

)

select * from final