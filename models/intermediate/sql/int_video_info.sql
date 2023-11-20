with 

video_info as (

    select * from {{ ref('stg_video_info') }}

),

youtube_categories as (

    select * from {{ ref('youtube_categories') }}

),

final as (

    select 

        video_id
        , title
        , published_date
        , channel_id
        , channel_title
        , youtube_categories.category_id
        , youtube_categories.category_name
        , trending_date
        , thumbnail_link
        , comments_disabled
        , ratings_disabled
        , description
        , dislikes_count

        , case 
            when tags = '[none]' then []
            else split(tags, '|')
            end as tags    

        from video_info
        left join youtube_categories using(category_id)

)

select * from final