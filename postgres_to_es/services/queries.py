main_query = """
    SELECT
        fw.id::text,
        fw.title,
        fw.description,
        fw.rating,
        fw.type,
        fw.created,
        fw.modified,
        COALESCE(ARRAY_AGG(DISTINCT g.name), ARRAY[]::text[]) AS genres,
        COALESCE(
            JSONB_AGG(
                DISTINCT jsonb_build_object(
                    'id', p.id::text,
                    'name', p.full_name,
                    'role', pfw.role
                )
            ) FILTER (WHERE p.id IS NOT NULL),
            '[]'::jsonb
        ) AS persons
    FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
    LEFT JOIN content.genre g ON gfw.genre_id = g.id
    LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
    LEFT JOIN content.person p ON pfw.person_id = p.id
    WHERE fw.modified > %s
    GROUP BY fw.id
    ORDER BY fw.modified
"""
