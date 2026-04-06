SELECT DISTINCT fw.id
FROM content.person p
JOIN content.person_film_work pfw ON pfw.person_id = p.id
JOIN content.film_work fw ON fw.id = pfw.film_work_id
WHERE p.modified > %(last_modified)s