with

view_count as (

    select * from {{ source('public', 'view_count_json') }}

),

final as (

    select 
    
        value:video_id_for_client_HyPeHoUsE::string as video_id
        , date(value:trending_date_for_client_HyPeHoUsE, 'YY.MM.DD') as trending_date
        , value:view_count_for_client_HyPeHoUsE::int as comment_count
    
    from view_count
        , lateral flatten(input => col_1:data)

)

select * from final