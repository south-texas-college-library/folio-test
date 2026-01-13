--metadb:function lib_banner_holds

DROP FUNCTION IF EXISTS lib_banner_holds;

CREATE FUNCTION lib_banner_holds(
    min_fee numeric DEFAULT 400.00,
    max_fee numeric DEFAULT 1000000.00
)
RETURNS TABLE(
    a_fee_date text,
    b_stc_id text,
    c_patron_profile text,
    d_username text,
    e_last_name text,
    f_first_name text,
    g_barcode text,
    h_item_title text,
    i_fee_balance text
)
AS $$
SELECT
    jsonb_extract_path_text(a.jsonb, 'metadata' , 'updatedDate')::date::text AS a_fee_date,
    jsonb_extract_path_text(u.jsonb, 'barcode') AS b_stc_id ,
    jsonb_extract_path_text(g.jsonb, 'group') AS c_patron_profile,
    jsonb_extract_path_text(u.jsonb, 'username') as d_username ,
    jsonb_extract_path_text(u.jsonb, 'personal', 'lastName') AS e_last_name ,
    jsonb_extract_path_text(u.jsonb, 'personal', 'firstName') AS f_first_name ,
    concat('<a href="https://stc-test.folio.indexdata.com/inventory?filters=staffSuppress.false&qindex=items.barcode&query=', jsonb_extract_path_text(i.jsonb, 'barcode'), '&segment=items&sort=title" target="_blank" rel="noopener">',jsonb_extract_path_text(i.jsonb, 'barcode'), '</a>') as g_barcode,
    jsonb_extract_path_text(a.jsonb, 'title') AS h_item_title,
    jsonb_extract_path_text(a.jsonb, 'remaining') AS i_fee_balance
FROM
    folio_feesfines.accounts AS a
    LEFT JOIN folio_users.users AS u
    ON u.id = jsonb_extract_path_text(a.jsonb, 'userId')::uuid
    LEFT JOIN folio_users.groups AS g
    ON g.id = jsonb_extract_path_text(u.jsonb, 'patronGroup')::uuid
    LEFT JOIN folio_inventory.item i 
    ON i.id = jsonb_extract_path_text(a.jsonb, 'itemId')::uuid
WHERE
    (jsonb_extract_path_text(a.jsonb, 'remaining')::numeric >= min_fee AND jsonb_extract_path_text(a.jsonb, 'remaining')::numeric <= max_fee)
ORDER BY
    a_fee_date DESC, d_username ASC, i_fee_balance DESC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;