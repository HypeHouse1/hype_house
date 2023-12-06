with

view_count as (

    select * from {{ source('youtube_hype_house', 'view_count_json') }}

)

, transformed as (

    select

        flattened.value:video_id_for_client_HyPeHoUsE::string              as video_id
        , {{ get_date_from_path('file_path') }} as trending_date
        , flattened.value:view_count_for_client_HyPeHoUsE                  as view_count

    from view_count
    , lateral flatten(input => json_data:data) as flattened

)

, final as (

    select

        video_id
        , trending_date
        , {{ cast_to_number('view_count') }} as view_count

    from transformed
    group by

        video_id
        , trending_date
        , view_count

)

select * from final
