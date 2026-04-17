WITH loans AS (
    SELECT
        l.item_id AS item_id,
        COUNT(l.id) AS checkouts,
        SUM(l.renewal_count) AS renewals
    FROM folio_circulation.loan__t l
    LEFT JOIN folio_inventory.item i ON i.id = l.item_id
    GROUP BY 
        l.item_id 
)
SELECT
    jsonb_extract_path_text(ins.jsonb, 'title') AS title,
    lc.name AS campus,
    jsonb_path_query_first(ins.jsonb, '$.contributors[*].name') #>> '{}' AS author,	
    hr.call_number AS call_number,
    jsonb_extract_path_text(it.jsonb , 'barcode') AS barcode,
    NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), '') AS price,
    mt.name AS material_type,
    jsonb_extract_path_text(ins.jsonb , 'catalogedDate') AS date_created,
    GREATEST(jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'start'), jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'end')) AS publication_date,
    NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', ' :.*?\$\d+\.\d{2}', '', 'g'), '[\[\]"]', '', 'g'), '') AS identifiers,
    hl.name AS home_location,
    il.name AS current_location,
    REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[\[\]"]', '', 'g') AS subjects,
    COALESCE(loans.checkouts, 0) AS checkouts,
    COALESCE(loans.renewals, 0) AS renewals,
    jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' AS publisher,
    jsonb_extract_path_text(insc.jsonb, 'name') AS subtype,
    jsonb_extract_path_text(itsc.jsonb, 'name') AS fund
FROM
    folio_inventory.instance ins
    JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
    JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
    JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
    JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
    JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
    JOIN folio_inventory.loccampus__t lc ON lc.id = hl.campus_id
    LEFT JOIN folio_inventory.statistical_code insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    LEFT JOIN folio_inventory.statistical_code itsc ON itsc.id = (jsonb_path_query_first(it.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    LEFT JOIN loans ON loans.item_id = it.id
WHERE 
    TO_TSVECTOR(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value')::text, '[\[\]"]', '', 'g')) @@ WEBSEARCH_TO_TSQUERY('math')