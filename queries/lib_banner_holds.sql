--metadb:function lib_banner_holds

DROP FUNCTION IF EXISTS lib_banner_holds;

CREATE FUNCTION lib_banner_holds(
    min_fee integer DEFAULT '400',
    max_fee integer DEFAULT '1000000'
)
RETURNS TABLE(
    a_fee_date text,
    b_stc_id text,
    c_username text,
    d_patron_profile text,
    e_item_title text,
    fee_balance numeric
)
AS $$
SELECT
    faa.transaction_date::date::text AS a_fee_date,
    u.barcode AS b_stc_id,
    u.username AS c_username,
    li.patron_group_name AS d_patron_profile,
    ihi.title AS e_item_title,
    faa.account_balance AS fee_balance
FROM
    folio_derived.feesfines_accounts_actions AS faa
    LEFT JOIN folio_derived.loans_items AS li
    ON faa.user_id = li.user_id
    LEFT JOIN folio_users.users__t AS u
    ON u.id = faa.user_id
    LEFT JOIN folio_derived.items_holdings_instances AS ihi
    ON ihi.barcode = li.barcode
WHERE
    (faa.account_balance >= min_fee AND faa.account_balance <= max_fee)
    AND (faa.fine_status = 'Open')
ORDER BY
    faa.transaction_date DESC, u.username ASC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;