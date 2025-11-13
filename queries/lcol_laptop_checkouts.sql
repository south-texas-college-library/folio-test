--metadb:function lcol_laptop_checkouts

DROP FUNCTION IF EXISTS lcol_laptop_checkouts;

CREATE FUNCTION lcol_laptop_checkouts(
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    campus_lab text,
    count_of_loans integer
)
AS $$
SELECT
    li.current_item_effective_location_name  AS campus_lab,
    count(li.loan_id) AS count_of_loans
FROM
    folio_derived.loans_items AS li
    LEFT JOIN folio_derived.items_holdings_instances ihi 
    ON ihi.item_id = li.item_id 
    LEFT JOIN folio_derived.instance_statistical_codes isc
    ON ihi.instance_id = isc.instance_id 
    LEFT JOIN folio_derived.locations_libraries AS ll
    ON li.current_item_effective_location_id = ll.location_id
WHERE
    isc.statistical_code_name  = 'Laptop' AND
    li.material_type_name  = 'DAYUSE-ITEM' AND
    date(li.loan_date) BETWEEN start_date AND end_date
GROUP BY
    li.current_item_effective_location_name
ORDER BY
    campus_lab ASC, count_of_loans DESC
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;