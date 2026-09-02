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
    "Item ID" TEXT,
    "Barcode" TEXT,
    "Call Number" TEXT,
    "Publication Date" TEXT,
    "Title" TEXT,
    "Author" TEXT,
    "ISBN" TEXT,
	"Publisher" TEXT,
    "Subjects" TEXT
)
AS $$
    WITH content AS (
        SELECT
            ins.id AS instance_id,
            sct.name as name
        FROM folio_inventory.instance ins
        JOIN folio_inventory.statistical_code__t sct ON sct.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
        JOIN folio_inventory.statistical_code_type__t sctt ON sctt.id = sct.statistical_code_type_id and sctt.name = 'CONTENT'
    ),
    subtype AS (
        SELECT
            ins.id AS instance_id,
            sct.name as name
        FROM folio_inventory.instance ins
        JOIN folio_inventory.statistical_code__t sct ON sct.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
        JOIN folio_inventory.statistical_code_type__t sctt ON sctt.id = sct.statistical_code_type_id and sctt.name = 'SUBTYPE'
    ),
    fund AS (
        SELECT
            it.id AS item_id,
            sct.name as name
        FROM folio_inventory.item it
        JOIN folio_inventory.statistical_code__t sct ON sct.id = (jsonb_path_query_first(it.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
        JOIN folio_inventory.statistical_code_type__t sctt ON sctt.id = sct.statistical_code_type_id and sctt.name = 'FUND'
    ),
    loans AS materialized (
        SELECT
            jsonb_extract_path_text(l.jsonb, 'itemId')::uuid AS item_id,
            jsonb_extract_path_text(l.jsonb, 'loanDate') AS loan_date,
            jsonb_extract_path_text(l.jsonb, 'dueDate') AS due_date
        FROM folio_circulation.loan l
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    ),
    stats AS materialized (
        SELECT
            l.item_id AS item_id,
            COUNT(l.id) AS checkouts,
            SUM(l.renewal_count) AS renewals
        FROM folio_circulation.loan__t l
        GROUP BY 
            l.item_id
    )
    SELECT
        it.id as "Item ID",
        jsonb_extract_path_text(it.jsonb, 'barcode') as "Barcode",
        hr.call_number as "Call Number",
        COALESCE(
            GREATEST(jsonb_extract_path_text(ins.jsonb, 'dates', 'date1'), jsonb_extract_path_text(ins.jsonb, 'dates', 'date2')),
            REGEXP_REPLACE(jsonb_path_query_first(ins.jsonb, '$.publication[*].dateOfPublication') #>> '{}', '[^0-9?,\s-]', '', 'g')
        ) as "Publication Date",
        jsonb_extract_path_text(ins.jsonb, 'title') as "Title",
        jsonb_path_query_first(ins.jsonb, '$.contributors[*].name') #>> '{}' as "Author",	
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', '\[|\]|"| :.*?\$\d+\.\d{2}', '', 'g'), '') as "ISBN",
        jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' as "Publisher",
        NULLIF(TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '') as "Subjects"
    FROM folio_inventory.instance ins
    JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
    JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
    JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
    JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
    JOIN folio_inventory.loccampus__t lc ON lc.id = hl.campus_id
    LEFT JOIN folio_inventory.location__t tl ON tl.id = it.temporarylocationid
    LEFT JOIN loans ON loans.item_id = it.id
    LEFT JOIN stats ON stats.item_id = it.id
    LEFT JOIN content ON content.instance_id = ins.id
    LEFT JOIN subtype ON subtype.instance_id = ins.id
    LEFT JOIN fund ON fund.item_id = it.id
    WHERE
        (subjects IS NULL OR TO_TSVECTOR('english', NULLIF(TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '')) @@ WEBSEARCH_TO_TSQUERY('english', subjects))
        AND (material_type = 'All' OR mt.name = material_type)
        AND (campus = 'All' OR lc.name = campus)
        AND CASE department
            WHEN 'Library' THEN (sub_type != 'All' OR COALESCE(hr.call_number, '') ~ '^[A-Z]{1,3}\s*[0-9]') AND hl.name !~* '(CLE|Open Lab)'
            WHEN 'CLE' THEN (sub_type != 'All' OR COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]') AND hl.name ~* '(CLE)'
            WHEN 'Open Lab' THEN (sub_type != 'All' OR COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]') AND hl.name ~* '(Open Lab)'
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
        AND (sub_type = 'All' OR subtype.name = sub_type)
    ORDER BY
        hr.call_number, jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;