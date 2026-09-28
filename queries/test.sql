--metadb:function get_users

DROP FUNCTION IF EXISTS get_users;

CREATE FUNCTION get_users(
    start_date date DEFAULT (CURRENT_DATE - INTERVAL '7 day'),
    end_date date DEFAULT (CURRENT_DATE - INTERVAL '1 day')
)
RETURNS TABLE(
    id uuid,
    barcode text,
    created_date timestamptz
)
AS $$
SELECT 
	id,
    barcode,
    created_date
FROM folio_users.users__t
WHERE start_date <= created_date AND created_date < end_date
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;