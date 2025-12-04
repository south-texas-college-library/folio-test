--metadb:function lib_banner_holds

DROP FUNCTION IF EXISTS lib_banner_holds;

CREATE FUNCTION lib_banner_holds(
    min_fee integer DEFAULT '400',
    max_fee integer DEFAULT '1000000'
)
RETURNS TABLE(
    a_number text,
    b_username text,
    c_patron_profile text,
    d_title text,
    fee_date date,
    g_balance numeric
)
AS $$
SELECT
    u.barcode AS a_number,
    u.username AS b_username,
    li.patron_group_name AS c_patron_profile,
    ihi.title AS d_title,
    date(faa.transaction_date) AS fee_date,
    faa.account_balance AS g_balance
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
    faa.account_balance ASC, u.username ASC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;