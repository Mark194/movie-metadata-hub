SELECT DISTINCT fw.id
FROM content.genre g
JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
JOIN content.film_work fw ON fw.id = gfw.film_work_id
WHERE g.modified > %(last_modified)s