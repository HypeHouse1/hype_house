with

likes_count as (

    select * from {{ source('youtube_hype_house', 'likes_count_json') }}

),

transformed as (

    select 
        
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , {{ get_date_from_path('file_path') }} as trending_date
        , value:likes_for_client_HyPeHoUsE as likes
        , value:dislikes_for_client_HyPeHoUsE as dislikes
    
    from youtube_raw.public.likes_count_json
        , lateral flatten(input => json_data:data)

),

final as (

    select

        video_id
        , trending_date
        , {{ cast_to_number('likes') }} as likes_count
        , {{ cast_to_number('dislikes') }} as dislikes_count

    from transformed
    group by 

        video_id
        , trending_date
        , likes_count
        , dislikes_count

)

select * from final