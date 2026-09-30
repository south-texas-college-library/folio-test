--metadb:function technology_checkouts

DROP FUNCTION IF EXISTS technology_checkouts;

CREATE FUNCTION technology_checkouts(
    subtype TEXT DEFAULT NULL,
    item_library TEXT DEFAULT NULL,
    po_number TEXT DEFAULT NULL
)
RETURNS TABLE(
    "Subtype" TEXT,
    "Title" TEXT,
    "Call Number" TEXT,
    "Item Library" TEXT,
    "Barcode" TEXT,
    "Status" TEXT,
    "Check Out Library" TEXT,
    "Due Date" TEXT,
    "User #" TEXT,
    "Name" TEXT,
    "Phone" TEXT,
	"Email" TEXT,
    "PO Number" TEXT,
    "Staff Notes" TEXT
)
AS $$
    WITH loans AS MATERIALIZED (
        SELECT
            it.id AS item_id,
            jsonb_extract_path_text(l.jsonb, 'dueDate') AS due_date,
            jsonb_extract_path_text(u.jsonb, 'barcode') AS user_barcode,
            NULLIF(CONCAT(jsonb_extract_path_text(u.jsonb, 'personal', 'firstName'), ' ', jsonb_extract_path_text(u.jsonb, 'personal', 'lastName')), ' ') AS full_name,
            jsonb_extract_path_text(u.jsonb, 'personal', 'phone') AS phone,
            jsonb_extract_path_text(u.jsonb, 'personal', 'email') AS email,
            jsonb_extract_path_text(lc.jsonb, 'name') AS checkout_campus
        FROM folio_inventory.instance ins
        JOIN folio_inventory.holdings_record hr ON hr.instanceid = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
        JOIN folio_circulation.loan l ON jsonb_extract_path_text(l.jsonb, 'itemId')::uuid = it.id
        JOIN folio_users.users u ON u.id = jsonb_extract_path_text(l.jsonb, 'userId')::uuid
        JOIN folio_inventory.location ll ON ll.id = jsonb_extract_path_text(l.jsonb, 'itemEffectiveLocationIdAtCheckOut')::uuid
        JOIN folio_inventory.loccampus lc on lc.id = jsonb_extract_path_text(ll.jsonb, 'campusId')::uuid
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    )
    SELECT
        insc.name AS "Subtype",
        jsonb_extract_path_text(ins.jsonb, 'title') AS "Title",
        jsonb_extract_path_text(hr.jsonb, 'callNumber') AS "Call Number",
        ll.name AS "Item Library",
        jsonb_extract_path_text(it.jsonb, 'barcode') AS "Barcode",
        jsonb_extract_path_text(it.jsonb, 'status', 'name') AS "Status",
        loans.checkout_campus AS "Check Out Library",
        loans.due_date::DATE::TEXT AS "Due Date",
        loans.user_barcode AS "User #",
        loans.full_name AS "Name",
        loans.phone AS "Phone",
        loans.email AS "Email",
        jsonb_path_query_first(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}' as "PO #",
        TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[]"', '') as "Staff Notes"
    FROM folio_inventory.instance ins
    JOIN folio_inventory.holdings_record hr ON hr.instanceid = ins.id
    JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanentlocationid
    JOIN folio_inventory.loclibrary__t ll ON ll.id = hl.library_id
    JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    JOIN folio_inventory.material_type__t m ON m.id = jsonb_extract_path_text(it.jsonb, 'materialTypeId')::uuid
    LEFT JOIN loans ON loans.item_id = it.id
    WHERE
        (item_library = 'All' OR ll.name = item_library)       
        AND CASE
                WHEN subtype = 'Calculator'
                    THEN insc.name = 'Calculator' AND m.name = 'SEM-ITEM'
                WHEN subtype = 'Hotspot'
                    THEN insc.name = 'Hotspot' AND m.name = 'SEMEXTEND-ITEM' AND hl.name != 'Storage'
                WHEN subtype = 'Laptop'
                    THEN insc.name = 'Laptop' AND m.name = 'SEMEXTEND-ITEM' 
                ELSE
                    (insc.name = 'Calculator' AND m.name = 'SEM-ITEM') 
                    OR (insc.name IN ('Laptop', 'Hotspot') AND m.name = 'SEMEXTEND-ITEM')
            END
        AND (po_number IS NULL OR jsonb_path_query_first(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}' ilike ('%' || po_number || '%'))
    ORDER BY
        jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;