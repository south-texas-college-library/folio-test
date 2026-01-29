-- Get total amount of checkouts from each item record

SELECT
    it.id AS id,
    count(l.id) AS loans
FROM
    folio_inventory.item it
LEFT JOIN folio_circulation.loan__t l ON l.item_id = it.id
GROUP BY 
    it.id