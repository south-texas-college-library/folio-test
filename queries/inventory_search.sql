--metadb:function inventory_search

DROP FUNCTION IF EXISTS inventory_search;

CREATE FUNCTION inventory_search(
    subject text DEFAULT NULL,
    start_cn text DEFAULT 'A',
    end_cn text DEFAULT 'ZZZ 9999.999'
) 
RETURNS TABLE(
    "A - Title" TEXT,
    "B - Campus" TEXT,
    "C - Author" TEXT,
    "D - Call Number" TEXT,
    "E - Item Barcode" TEXT,
    "F - Item Price" TEXT,
    "G - Item Type" TEXT,
    "H - Date Created" TEXT,
    "I - Publication Date" TEXT,
    "J - Identifiers" TEXT,
    "K - Home Location" TEXT,
    "L - Current Location" TEXT,
    "M - Subjects" TEXT,
    "N - Total Renewals" INTEGER,
    "O - Total Checkouts" INTEGER,
	"P - Publisher" TEXT,
    "Q - Content" TEXT,
    "R - Subtype" TEXT
)
AS $$
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
        jsonb_extract_path_text(ins.jsonb, 'title'),
        lc.name,
        jsonb_path_query_first(ins.jsonb, '$.contributors[*].name') #>> '{}',	
        hr.call_number,
        jsonb_extract_path_text(it.jsonb , 'barcode'),
        jsonb_extract_path_text(it.jsonb , 'status', 'name'),
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), ''),
        mt.name,
        jsonb_extract_path_text(ins.jsonb , 'catalogedDate'),
        GREATEST(jsonb_extract_path_text(ins.jsonb, 'dates', 'date1'), jsonb_extract_path_text(ins.jsonb, 'dates', 'date2')) AS "Publicated Date",
        NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', ' :.*?\$\d+\.\d{2}', '', 'g'), '[\[\]"]', '', 'g'), '') AS "Identifiers",
        hl.name AS "Home Location",
        il.name AS "Current Location",
        REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[\[\]"]', '', 'g') AS "Subjects",
        COALESCE(loans.checkouts, 0) AS "Checkouts",
        COALESCE(loans.renewals, 0) AS "Renewals",
        jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' AS "Publisher",
        jsonb_extract_path_text(insc.jsonb, 'name') AS "Subtype",
        jsonb_extract_path_text(itsc.jsonb, 'name') AS "Fund"
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
        hr.call_number ~ '^[A-Z]{1,3}\s*[0-9]'
        AND hr.call_number between start_cn and end_cn
        AND (subject IS NULL OR TO_TSVECTOR('english', REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value')::text, '[\[\]"]', '', 'g')) @@ WEBSEARCH_TO_TSQUERY('english', subject))
    $$
LANGUAGE SQL
STABLE
PARALLEL SAFE;