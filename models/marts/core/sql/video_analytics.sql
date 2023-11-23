with

comment_count as (

    select * from {{ ref('stg_comment_count') }}

)

, likes_count as (

    select * from {{ ref('stg_likes_count') }}

)

, view_count as (

    select * from {{ ref('stg_view_count') }}

)

, video_info as (

    select * from {{ ref('stg_video_info') }}

)

, youtube_categories as (

    select * from {{ ref('youtube_categories') }}

)

, final as (

    select

        video_info.video_id
        , video_info.trending_date
        
        , published_date
        , title
        , channel_id
        , channel_title
        , youtube_categories.category_id
        , youtube_categories.category_name
        
        , case 
            when tags = '[none]' then []
            else split(tags, '|')
            end 
        as tags   
        
        , thumbnail_link
        , comments_disabled
        , ratings_disabled
        , description

        , comment_count.comment_count as comment_count
        , view_count.view_count as view_count
        , likes_count.likes_count as likes_count

    from video_info
    
    left join comment_count 
        on video_info.video_id = comment_count.video_id
        and video_info.trending_date = comment_count.trending_date

    left join view_count 
        on video_info.video_id = view_count.video_id
        and video_info.trending_date = view_count.trending_date
    
    left join likes_count
        on video_info.video_id = likes_count.video_id
        and video_info.trending_date = likes_count.trending_date
    
    left join youtube_categories
        on video_info.category_id = youtube_categories.category_id

)

select * from final