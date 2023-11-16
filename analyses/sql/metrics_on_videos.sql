with 

video_analytics as (

    select * from {{ ref('video_analytics') }}

),

final as (

    select 

        video_id
        , trending_date
        , round(div0(likes_count, likes_count + dislikes_count)*100, 2) as likes_percentage
        , round(div0(dislikes_count, likes_count + dislikes_count)*100, 2) as dislikes_percentage

        , round(div0(likes_count, view_count), 4) as likes_over_views
        , round(div0(dislikes_count, view_count), 4) as dislikes_over_views
        , round(div0(comment_count, view_count), 4) as comments_over_views

        , round(div0(comment_count, likes_count), 4) as comments_over_likes
        , round(div0(comment_count, dislikes_count), 4) as comments_over_dislikes
        
    
    from video_analytics

)

select * from final