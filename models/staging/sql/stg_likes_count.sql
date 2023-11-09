with

likes_count as (

    select * from {{ source('youtube_hype_house', 'likes_count_json') }}

),

transformed as (

    select 
        
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , date(value:trending_date_for_client_HyPeHoUsE, 'YY.MM.DD') as trending_date
        , value:likes_for_client_HyPeHoUsE as likes
        , value:dislikes_for_client_HyPeHoUsE as dislikes
    
    from youtube_raw.public.likes_count_json
        , lateral flatten(input => col_1:data)

),

final as (

    select

        video_id
        , trending_date
        , {{ cast_to_number('likes') }} as likes
        , {{ cast_to_number('dislikes') }} as dislikes

    from transformed

)

select * from final