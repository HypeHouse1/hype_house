{% snapshot video_snapshot %}

    {{
        config(
            target_database='youtube_dev',
            target_schema='snapshots',
            unique_key='video_id',

            strategy='timestamp',
            updated_at='trending_date',
        )
    }}


    {# {{
      config(
        target_database='youtube_dev',
        target_schema='snapshots',
        unique_key='file_path',

        strategy='check',
        check_cols='all',
        )
    }} #}

    select * from {{ ref('stg_video_info') }}


{% endsnapshot %}