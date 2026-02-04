-- Get total amount of checkouts from each item record

SELECT
    it.id AS item_id,
    count(l.id) AS checkouts
FROM
    folio_inventory.item it
JOIN folio_circulation.loan__t l ON l.item_id = it.id
GROUP BY 
    it.id