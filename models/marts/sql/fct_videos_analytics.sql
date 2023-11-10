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

    select * from {{ ref('stg_video_info') }}

),

analytics as (

    select

        video_info.video_id as video_id
        , sum(comment_count.comment_count) as comment_count
        , sum(view_count.view_count) as view_count
        , sum(likes_count.likes_count) as likes_count
        , sum(video_info.dislikes + likes_count.dislikes_count) as dislikes_count

    from video_info
    left join comment_count
        on video_info.video_id = comment_count.video_id
    left join view_count
        on video_info.video_id = view_count.video_id
    left join likes_count
        on video_info.video_id = likes_count.video_id
    
    group by video_info.video_id

),

final as (

    select 
    
        analytics.video_id
        , title
        , published_date
        , channel_id
        , channel_title
        , category_id
        , trending_date
        , tags
        , thumbnail_link
        , comments_disabled
        , ratings_disabled
        , description
        , likes_count
        , dislikes_count
        , comment_count
        , view_count
    
    from analytics
    left join video_info
        on analytics.video_id = video_info.video_id

)

select * from final