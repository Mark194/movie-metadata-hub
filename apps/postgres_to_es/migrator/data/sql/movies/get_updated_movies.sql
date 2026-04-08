SELECT
    fw.id,
    fw.title,
    fw.rating,
    fw.description,
    fw.modified,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'id', p.id,
            'full_name', p.full_name,
            'role', pfw.role
        )) FILTER (WHERE p.id IS NOT NULL),
        '[]'::json
    ) as persons,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'id', g.id,
            'name', g.name
        )) FILTER (WHERE g.id IS NOT NULL),
        '[]'::json
    ) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > %(last_modified)s
GROUP BY fw.id
ORDER BY fw.modified
LIMIT %(limit)s