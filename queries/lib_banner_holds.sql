--metadb:function lib_banner_holds

DROP FUNCTION IF EXISTS lib_banner_holds;

CREATE FUNCTION lib_banner_holds(
    min_fee integer DEFAULT '400',
    max_fee integer DEFAULT '1000000'
)
RETURNS TABLE(
    a_fee_date text,
    b_stc_id text,
    c_patron_profile text,
    d_username text,
    e_last_name text,
    f_first_name text,
    g_item_title text,
    h_fee_balance numeric
)
AS $$
SELECT
    faa.transaction_date::date::text AS a_fee_date,
    jsonb_extract_path_text(u.jsonb, 'barcode') AS b_stc_id ,
    faa.patron_group_name AS c_patron_profile,
    jsonb_extract_path_text(u.jsonb, 'username') as d_username ,
    jsonb_extract_path_text(u.jsonb, 'personal', 'lastName') AS e_last_name ,
    jsonb_extract_path_text(u.jsonb, 'personal', 'firstName') AS f_first_name ,    
    ihi.title AS g_item_title,
    faa.account_balance AS h_fee_balance
FROM
    folio_derived.feesfines_accounts_actions AS faa
    LEFT JOIN folio_users.users__ AS u
    ON u.id = faa.user_id
    LEFT JOIN = folio_derived.loans_items AS li
    ON faa.user_id = li.user_id
    LEFT JOIN folio_derived.items_holdings_instances AS ihi
    ON ihi.barcode = li.barcode
WHERE
    (faa.account_balance >= min_fee AND faa.account_balance <= max_fee)
    AND (faa.fine_status = 'Open')
ORDER BY
    fee_date DESC, username ASC, fee_balance DESC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;