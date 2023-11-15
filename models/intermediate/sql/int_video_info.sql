with 

video_info as (

    select * from {{ ref('stg_video_info') }}

),

final as (

    select 

        video_id
        , title
        , published_date
        , channel_id
        , channel_title
        , category_id
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

)

select * from final