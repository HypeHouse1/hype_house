with

channels_multiple_trending_videos as (

    select * from {{ ref('int_channels_multiple_trending_videos') }}

),

final as (

    select

        channel_id

        , round(avg(array_size(tags)), 2) as avg_tags_count

        , round(avg(likes_count), 2) as avg_likes_count
        , round(avg(comment_count), 2) as avg_comment_count
        , round(avg(view_count), 2) as avg_view_count

    from channels_multiple_trending_videos

    group by

        channel_id

)

select * from final