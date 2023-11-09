with

comment_count as (

    select * from {{ source('youtube_hype_house', 'comment_count_json') }}

),

transformed as (

    select 
        
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , date(value:trending_date_for_client_HyPeHoUsE, 'YY.MM.DD') as trending_date
        , value:comment_count_for_client_HyPeHoUsE as comment_count

    from comment_count
        , lateral flatten(input => col_1:data)

),

final as (

    select 

        video_id
        , trending_date
        , {{ cast_to_number('comment_count') }} as comment_count

    from transformed

)

select * from final