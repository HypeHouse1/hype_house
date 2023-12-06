with

channels_trending_consecutive_days as (

    select * from {{ ref('int_channels_trending_consecutive_days') }}

)

, final as (

    select

        channel_id
        , video_id
        , trending_date

        , likes_count

        , comment_count
        , view_count
        , array_size(tags) as tags_count

        , {{ difference_from_previous('tags_count', 'video_id', 'trending_date') }} as diff_tags

        , {{ difference_from_previous('likes_count', 'video_id', 'trending_date') }} as diff_likes

        , {{ difference_from_previous('comment_count', 'video_id', 'trending_date') }} as diff_comments

        , {{ difference_from_previous('view_count', 'video_id', 'trending_date') }} as diff_views

    from channels_trending_consecutive_days
)

select * from final
