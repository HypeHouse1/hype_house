with

video_info as (

    select * from {{ source('youtube_hype_house', 'video_info_json') }}

),

transformed as (

    select 
        
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , value:title_for_client_HyPeHoUsE::string as title
        , date(value:publishedAt_for_client_HyPeHoUsE) as published_date
        , value:channelId_for_client_HyPeHoUsE::string as channel_id
        , value:channelTitle_for_client_HyPeHoUsE::string as channel_title
        , value:categoryId_for_client_HyPeHoUsE::int category_id
        , date(value:trending_date_for_client_HyPeHoUsE, 'YY.DD.MM') as trending_date
        , value:tags_for_client_HyPeHoUsE::string as tags
        , value:thumbnail_link_for_client_HyPeHoUsE::string as thumbnail_link
        , value:comments_disabled_for_client_HyPeHoUsE::boolean as comments_disabled
        , value:ratings_disabled_for_client_HyPeHoUsE::boolean as ratings_disabled
        , value:description_for_client_HyPeHoUsE::string as description
        , value:dislikes_for_client_HyPeHoUsE as dislikes

    from youtube_raw.public.video_info_json
        , lateral flatten(input => json_data:data)

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
        , tags
        , thumbnail_link
        , comments_disabled
        , ratings_disabled
        , description
        , {{ cast_to_number('dislikes') }} as dislikes

    from transformed

)

select * from final