with

likes_count as (

    select * from {{ source('youtube_hype_house', 'likes_count_json') }}

)

, transformed as (

    select

        flattened.value:video_id_for_client_HyPeHoUsE::string              as video_id
        , {{ get_date_from_path('file_path') }} as trending_date
        , flattened.value:likes_for_client_HyPeHoUsE                       as likes_count

    from likes_count
    , lateral flatten(input => json_data:data) as flattened

)

, final as (

    select

        video_id
        , trending_date
        , {{ cast_to_number('likes_count') }} as likes_count

    from transformed
    group by

        video_id
        , trending_date
        , likes_count

)

select * from final
