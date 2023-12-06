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

        , video_info.published_date
        , video_info.title
        , video_info.channel_id
        , video_info.channel_title

        , youtube_categories.category_id
        , youtube_categories.category_name

        , video_info.thumbnail_link

        , video_info.comments_disabled
        , video_info.ratings_disabled
        , video_info.description
        , comment_count.comment_count as comment_count

        , view_count.view_count       as view_count
        , likes_count.likes_count     as likes_count
        , case
            when video_info.tags = '[none]' then []
            else split(video_info.tags, '|')
        end
            as tags

    from video_info

    left join comment_count
        on
            video_info.video_id = comment_count.video_id
            and video_info.trending_date = comment_count.trending_date

    left join view_count
        on
            video_info.video_id = view_count.video_id
            and video_info.trending_date = view_count.trending_date

    left join likes_count
        on
            video_info.video_id = likes_count.video_id
            and video_info.trending_date = likes_count.trending_date

    left join youtube_categories
        on video_info.category_id = youtube_categories.category_id

)

select * from final
