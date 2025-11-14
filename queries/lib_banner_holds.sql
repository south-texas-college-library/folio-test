--metadb:function lib_banner_holds

DROP FUNCTION IF EXISTS lib_banner_holds;

CREATE FUNCTION lib_banner_holds(
    min_fee integer DEFAULT '20',
    max_fee integer DEFAULT '400'
)
RETURNS TABLE(
    a_number text,
    username text,
    patron_profile text,
    title text,
    fee_date timestamptz,
    balance numeric
)
AS $$
SELECT
    u.barcode AS a_number,
    u.username,
    li.patron_group_name AS patron_profile,
    ihi.title AS title,
    faa.transaction_date AS fee_date,
    faa.account_balance AS balance
FROM
    folio_derived.feesfines_accounts_actions AS faa
    LEFT JOIN folio_derived.loans_items AS li
    ON faa.user_id = li.user_id
    LEFT JOIN folio_users.users__t AS u
    ON u.id = faa.user_id
    LEFT JOIN folio_derived.items_holdings_instances AS ihi
    ON ihi.barcode = li.barcode
WHERE
    (li.permanent_loan_type_name = 'High Value')
    AND (faa.account_balance BETWEEN min_fee AND max_fee)
    AND (faa.fine_status = 'Open')
ORDER BY
    faa.account_balance ASC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;