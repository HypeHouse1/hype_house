with 

channels_trending_consecutive_days as (

    select * from {{ ref('int_channels_trending_consecutive_days') }}

)

, final as (

    select

        channel_id
        , video_id
        , trending_date

        , array_size(tags) as tags_count
        
        , likes_count
        , comment_count
        , view_count

        , tags_count - lag(tags_count, 1, 0) over (partition by video_id order by trending_date asc) as diff_tags

        , likes_count - lag(likes_count, 1, 0) over (partition by video_id order by trending_date asc) as diff_likes
        , comment_count - lag(comment_count, 1, 0) over (partition by video_id order by trending_date asc) as diff_comments
        , view_count - lag(view_count, 1, 0) over (partition by video_id order by trending_date asc) as diff_views

    from channels_trending_consecutive_days
)

select * from final