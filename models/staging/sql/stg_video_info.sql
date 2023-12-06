with

video_info as (

    select * from {{ source('youtube_hype_house', 'video_info_json') }}

)

, transformed as (

    select

        flattened.value:video_id_for_client_HyPeHoUsE::string                  as video_id
        , flattened.value:title_for_client_HyPeHoUsE::string                   as title
        , flattened.value:channelId_for_client_HyPeHoUsE::string               as channel_id
        , flattened.value:channelTitle_for_client_HyPeHoUsE::string            as channel_title
        , flattened.value:categoryId_for_client_HyPeHoUsE::int                 as category_id
        , flattened.value:tags_for_client_HyPeHoUsE::string                    as tags
        , flattened.value:thumbnail_link_for_client_HyPeHoUsE::string          as thumbnail_link
        , flattened.value:comments_disabled_for_client_HyPeHoUsE::boolean      as comments_disabled
        , flattened.value:ratings_disabled_for_client_HyPeHoUsE::boolean       as ratings_disabled
        , flattened.value:description_for_client_HyPeHoUsE::string             as description
        , date(flattened.value:publishedAt_for_client_HyPeHoUsE)               as published_date
        , date(flattened.value:trending_date_for_client_HyPeHoUsE, 'YY.DD.MM') as trending_date

    from video_info
    , lateral flatten(input => json_data:data) as flattened

)

, final as (

    select distinct

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

    from transformed

)

select * from final
{% if is_incremental() -%}
    where trending_date > (select max(trending_date) as trending_date from {{ this }})
{%- endif %}
