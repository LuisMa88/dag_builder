select 
    user_id, 
    id, 
    title, 
    char_length(title) AS title_size, 
    body, 
    char_length(body) AS body_size 
from 
    ingest_api_data.posts