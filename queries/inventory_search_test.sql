--metadb:function inventory_search_test

DROP FUNCTION IF EXISTS inventory_search_test;

CREATE FUNCTION inventory_search_test(
    start_cn TEXT DEFAULT '0',
    end_cn TEXT DEFAULT 'ZZZZZZZZ',
    subjects TEXT DEFAULT NULL,
    title TEXT DEFAULT NULL,
    search_type TEXT DEFAULT NULL,
    material_type TEXT DEFAULT NULL,
    sub_type TEXT DEFAULT NULL,
    campus TEXT DEFAULT NULL,
    department TEXT DEFAULT NULL
)
RETURNS TABLE(
    "Barcode" TEXT,
    "Call Number" TEXT,
    "Publication Date" TEXT,
    "Title" TEXT,
    "Author" TEXT,
    "ISBN" TEXT,
	"Publisher" TEXT,
    "Subjects" TEXT,
    "Cataloged Date" TEXT,
    "Subtype" TEXT,
    "Department" TEXT,
    "Campus" TEXT,
    "Location" TEXT,
    "Temporary Location" TEXT,
    "Material Type" TEXT,
    "Created Date" TEXT,
    "Inventory Date" TEXT,
    "PO Number" TEXT,
    "Invoice" TEXT,
    "Ownership" TEXT,
    "Price" TEXT,
    "Public Notes" TEXT,
    "Circulation Notes" TEXT,
    "Staff Notes" TEXT,
    "Status" TEXT
)
AS $$
    WITH inventory as materialized (      
        SELECT
            ins.id as instance_id,
            it.id as item_id,
            ins.jsonb as instance_jsonb,
            it.jsonb as item_jsonb,
            hr.call_number as call_number,
            sct.name as subtype,
            CASE
                WHEN hl.name ~* 'CLE' THEN 'CLE'
                WHEN hl.name ~* 'Open Lab' THEN 'Open Lab'
                ELSE 'Library'
            END AS department,
            lc.name as campus,
            hl.name as location,
            tl.name as temporary_location,
            mt.name as material_type
        FROM folio_inventory.instance ins
        JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
        JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
        JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
        JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
        JOIN folio_inventory.loccampus__t lc ON lc.id = hl.campus_id
        LEFT JOIN folio_inventory.location__t tl ON tl.id = it.temporarylocationid
        LEFT JOIN folio_inventory.statistical_code__t sct ON sct.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
        LEFT JOIN folio_inventory.statistical_code_type__t sctt on sctt.id = sct.statistical_code_type_id and sctt.name = 'SUBTYPE'
        WHERE
            (subjects IS NULL OR TO_TSVECTOR('english', NULLIF(TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '')) @@ WEBSEARCH_TO_TSQUERY('english', subjects))
            AND (material_type = 'All' OR mt.name = material_type)
            AND (campus = 'All' OR lc.name = campus)
            AND CASE department
                WHEN 'Library' THEN COALESCE(hr.call_number, '') ~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name !~* '(CLE|Open Lab)'
                WHEN 'CLE' THEN COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name ~* '(CLE)'
                WHEN 'Open Lab' THEN COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name ~* '(Open Lab)'
                ELSE TRUE
            END
            AND CASE search_type
                WHEN 'Contains All' THEN (title IS NULL OR TO_TSVECTOR('english', jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('english', title))
                WHEN 'Exact Match' THEN (title IS NULL OR TO_TSVECTOR(jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('"' || title || '"'))
                WHEN 'Contains Any' THEN (title IS NULL OR TO_TSVECTOR('english', jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('english', replace(title, ' ', ' OR ' )))
                WHEN 'Starts With' THEN (title IS NULL OR jsonb_extract_path_text(ins.jsonb, 'title') ILIKE (title || '%'))
                WHEN 'Ends With' THEN (title IS NULL OR jsonb_extract_path_text(ins.jsonb, 'title') ILIKE ('%' || title))
                ELSE TRUE
            END
            AND (hr.call_number IS NULL OR hr.call_number between start_cn and end_cn)
            AND (sub_type = 'All' OR sct.name = sub_type)
    ),
    codes AS (
        SELECT
            inv.instance_id AS instance_id,
            MAX(sct.name) FILTER (WHERE sctt.name = 'CONTENT') AS content,
            MAX(sct.name) FILTER (WHERE sctt.name = 'SUBTYPE') AS subtype,
            MAX(sct.name) FILTER (WHERE sctt.name = 'FUND') AS fund
        from inventory inv
        CROSS JOIN LATERAL jsonb_array_elements_text(
            jsonb_path_query_array(inv.instance_jsonb, '$.statisticalCodeIds[*]') || jsonb_path_query_array(inv.item_jsonb, '$.statisticalCodeIds[*]')
        ) stat_id
        JOIN folio_inventory.STATISTICAL_CODE__T sct ON sct.id = stat_id::uuid
        JOIN folio_inventory.STATISTICAL_CODE_TYPE__T sctt ON sctt.id = sct.statistical_code_type_id
        GROUP BY 
            inv.instance_id
    ),
    loans AS (
        SELECT
            jsonb_extract_path_text(l.jsonb, 'itemId')::uuid AS item_id,
            jsonb_extract_path_text(l.jsonb, 'loanDate') AS loan_date,
            jsonb_extract_path_text(l.jsonb, 'dueDate') AS due_date
        FROM inventory inv 
        JOIN folio_circulation.loan l on jsonb_extract_path_text(l.jsonb, 'itemId')::uuid = inv.item_id
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    ),
    stats as (
        SELECT
            l.item_id AS item_id,
            COUNT(l.id) AS checkouts,
            SUM(l.renewal_count) AS renewals
        FROM inventory inv 
        JOIN folio_circulation.loan__t l on l.item_id = inv.item_id
        GROUP BY 
            l.item_id
    )
    SELECT
        jsonb_extract_path_text(inv.item_jsonb, 'barcode') as "Barcode",
        inv.call_number as "Call Number",
        COALESCE(
            GREATEST(jsonb_extract_path_text(inv.instance_jsonb, 'dates', 'date1'), jsonb_extract_path_text(inv.instance_jsonb, 'dates', 'date2')),
            REGEXP_REPLACE(jsonb_path_query_first(inv.instance_jsonb, '$.publication[*].dateOfPublication') #>> '{}', '[^0-9?,\s-]', '', 'g')
        ) as "Publication Date",
        jsonb_extract_path_text(inv.instance_jsonb, 'title') as "Title",
        jsonb_path_query_first(inv.instance_jsonb, '$.contributors[*].name') #>> '{}' as "Author",	
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(inv.instance_jsonb, '$.identifiers[*].value') #>> '{}', '\[|\]|"| :.*?\$\d+\.\d{2}', '', 'g'), '') as "ISBN",
        jsonb_path_query_first(inv.instance_jsonb, '$.publication[*].publisher') #>> '{}' as "Publisher",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.instance_jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '') as "Subjects",
        jsonb_extract_path_text(inv.instance_jsonb, 'catalogedDate') as "Cataloged Date",
        inv.subtype as "Subtype",
        inv.department AS "Department",
        inv.campus as "Campus",
        inv.location as "Location",
        inv.temporary_location as "Temporary Location",
        inv.material_type as "Material Type",
        COALESCE(
            TO_DATE(split_part(jsonb_path_query_first(inv.item_jsonb, '$.administrativeNotes[*]') #>> '{}', ': ', 2), 'YYYYMMDD')::text,
            jsonb_extract_path_text(inv.item_jsonb, 'metadata', 'createdDate')::date::text
        ) as "Created Date",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "e1f34ba3-6d37-462e-878c-17f922b13d93").note') #>> '{}', '[]"', ''), '') AS "Inventory Date",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[]"', ''), '') AS "PO Number",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "8f99bd3a-706c-45d2-89d8-8eca7fa1c03f").note') #>> '{}', '[]"', ''), '') AS "Invoice",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "34207e4e-5cd7-4eab-801b-b0326cd5c66a").note') #>> '{}', '[]"', ''), '') AS "Ownership",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[]"', ''), '') AS "Price",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "b6b35579-ee2b-4973-8e0d-ebc05bab0dab").note') #>> '{}', '[]"', ''), '') AS "Public Notes",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5366d4d4-8775-4cf4-a00f-77c82f0ca3bf").note') #>> '{}', '[]"', ''), '') AS "Circulation Notes",
        NULLIF(TRANSLATE(jsonb_path_query_array(inv.item_jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[]"', ''), '') AS "Staff Notes",
        COALESCE(stats.checkouts, 0) as "Checkouts",
        COALESCE(stats.renewals, 0) as "Renewals",
        jsonb_extract_path_text(inv.item_jsonb, 'status', 'name') as "Status",
        loans.loan_date as "Loan Date",
        loans.due_date as "Due Date"
    FROM inventory inv
    LEFT JOIN loans ON loans.item_id = inv.item_id
    LEFT JOIN stats ON stats.item_id = inv.item_id
    LEFT JOIN codes ON codes.instance_id = inv.instance_id
    ORDER BY
        inv.call_number, jsonb_extract_path_text(inv.item_jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;