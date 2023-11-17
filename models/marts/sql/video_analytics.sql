with

comment_count as (

    select * from {{ ref('stg_comment_count') }}

),

likes_count as (

    select * from {{ ref('stg_likes_count') }}

),

view_count as (

    select * from {{ ref('stg_view_count') }}

),

video_info as (

    select * from {{ ref('int_video_info') }}

),

final as (

    select

        video_info.video_id
        , video_info.trending_date
        
        , published_date
        , title
        , channel_id
        , channel_title
        , category_id
        , category_name
        , tags
        , thumbnail_link
        , comments_disabled
        , ratings_disabled
        , description

        , comment_count.comment_count as comment_count
        , view_count.view_count as view_count
        , likes_count.likes_count as likes_count
        , video_info.dislikes_count as dislikes_count

    from video_info
    left join comment_count using (video_id, trending_date)
    left join view_count using (video_id, trending_date)
    left join likes_count using (video_id, trending_date)

)

select * from final