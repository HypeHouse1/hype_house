with

channels_multiple_trending_videos as (

    select * from {{ ref('int_channels_multiple_trending_videos') }}

)

, channels_multiple_trending as (

    select

        channel_id
        , video_id

        , array_agg(trending_date) as trending_dates

    from channels_multiple_trending_videos

    group by 

        channel_id
        , video_id
        , partition_id

)

, channels_multiple_trending_gaps as (

    select

        channel_id
        , video_id

        , array_union_agg(trending_dates) as trending_dates
    
    from channels_multiple_trending

    group by 

        channel_id
        , video_id
    
    having count(*) > 1

)

, channels_videos_multiple_trending_gaps as (

    select
        
        channel_id
        , video_id
        , value::date as trending_date

    from channels_multiple_trending_gaps,
        lateral flatten(input => channels_multiple_trending_gaps.trending_dates)

)

, channels_videos_multiple_trending_gaps_dates as (

    select

        channel_id
        , video_id
        , trending_date
        , lag(trending_date, 1, trending_date) over (partition by video_id order by trending_date) as previous_trending_date
    
    from channels_videos_multiple_trending_gaps

)

, final as (

    select

        channels_videos_multiple_trending_gaps_dates.channel_id
        , channels_videos_multiple_trending_gaps_dates.video_id
        , channels_videos_multiple_trending_gaps_dates.trending_date

        , array_size(tags) as tags_count

        , likes_count
        , comment_count
        , view_count

        , {{ difference_from_previous('tags_count', 'channels_videos_multiple_trending_gaps_dates.video_id', 'channels_videos_multiple_trending_gaps_dates.trending_date')}} as diff_tags

        , {{ difference_from_previous('likes_count', 'channels_videos_multiple_trending_gaps_dates.video_id', 'channels_videos_multiple_trending_gaps_dates.trending_date')}} as diff_likes

        , {{ difference_from_previous('comment_count', 'channels_videos_multiple_trending_gaps_dates.video_id', 'channels_videos_multiple_trending_gaps_dates.trending_date')}} as diff_comments

        , {{ difference_from_previous('view_count', 'channels_videos_multiple_trending_gaps_dates.video_id', 'channels_videos_multiple_trending_gaps_dates.trending_date')}} as diff_views

        , datediff(day, previous_trending_date, channels_videos_multiple_trending_gaps_dates.trending_date) as days_of_gap_from_previous

    from channels_videos_multiple_trending_gaps_dates

    left join int_channels_multiple_trending_videos
        on channels_videos_multiple_trending_gaps_dates.video_id = int_channels_multiple_trending_videos.video_id
        and channels_videos_multiple_trending_gaps_dates.trending_date = int_channels_multiple_trending_videos.trending_date
    
    where 1 = 1
        and (days_of_gap_from_previous = 0 or days_of_gap_from_previous > 1)

)

select * from final