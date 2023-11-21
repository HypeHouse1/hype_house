with

channels_multiple_trending_videos as (

    select * from {{ ref('int_channels_multiple_trending_videos') }}

)

, channels_multiple_trending as (

    select

        channel_id
        , channel_title
        , video_id

        , array_agg(trending_date) within group (order by trending_date) as trending_dates

    from channels_multiple_trending_videos

    group by 

        channel_id
        , channel_title
        , video_id
        , partition_id

)

, channels_multiple_trending_gaps as (

    select

        channel_id
        , channel_title
        , video_id

        , array_union_agg(trending_dates) as trending_dates
    
    from channels_multiple_trending

    group by 

        channel_id
        , channel_title
        , video_id
    
    having count(*) > 1

)

, channels_videos_multiple_trending_gaps as (

    select
        
        channel_id
        , channel_title
        , video_id
        , value::date as trending_date

    from channels_multiple_trending_gaps,
        lateral flatten(input => channels_multiple_trending_gaps.trending_dates)

)

, final as (

    select

        channels_videos_multiple_trending_gaps.channel_id
        , channels_videos_multiple_trending_gaps.channel_title
        , channels_videos_multiple_trending_gaps.video_id
        , channels_videos_multiple_trending_gaps.trending_date

        , likes_count
        , dislikes_count
        , comment_count
        , view_count

    from channels_videos_multiple_trending_gaps

    left join int_channels_multiple_trending_videos
        on channels_videos_multiple_trending_gaps.video_id = int_channels_multiple_trending_videos.video_id
        and channels_videos_multiple_trending_gaps.trending_date = int_channels_multiple_trending_videos.trending_date

)

select * from final
order by channel_id, video_id, trending_date