with

likes_count as (

    select * from {{ source('public', 'likes_count_json') }}

),

converted as (

    select 
        
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , date(value:trending_date_for_client_HyPeHoUsE, 'YY.MM.DD') as trending_date
        , value:likes_for_client_HyPeHoUsE as likes -- ifnull
        , value:dislikes_for_client_HyPeHoUsE as dislikes
    
    from youtube_raw.public.likes_count_json
        , lateral flatten(input => col_1:data)

),

final as (

    select 

        video_id
        , trending_date
        , zeroifnull(likes)::int as likes
        , zeroifnull(dislikes)::int as dislikes

    from converted

)

select * from final