--metadb:function get_semester_loans

DROP FUNCTION IF EXISTS get_semester_loans;

CREATE FUNCTION get_semester_loans(
    asset_type text DEFAULT NULL,
    item_library text DEFAULT NULL
)
RETURNS TABLE(
    "A - Asset ID" TEXT,
    "B - Statistical Code" TEXT,
    "C - User Barcode" TEXT,
    "D - Name" TEXT,
    "E - Phone Number" TEXT,
	"F - Email" TEXT,
    "G - Staff Notes" TEXT,
    "H - Item Library" TEXT,
    "I - Check Out Library" TEXT,
    "J - Status" TEXT
    "K - Due Date" TIMESTAMPTZ
)
AS $$
    WITH loans AS (
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
		join folio_inventory.loccampus lc on lc.id = jsonb_extract_path_text(ll.jsonb, 'campusId')::uuid
		WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    )
    SELECT
        jsonb_extract_path_text(it.jsonb, 'barcode') as "Asset ID",
        insc.name as "Statistical Code",
        loans.user_barcode as "User Barcode",
        loans.full_name as "Name",
        loans.phone as "Phone Number",
        loans.email as "Email",
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g'), '') as "Staff Notes",
        ll.name as "Item Library",
        loans.checkout_campus as "Check Out Library",
        jsonb_extract_path_text(it.jsonb, 'status', 'name') as "Status",
        loans.due_date::date::text as "Due Date"
    FROM folio_inventory.instance ins
    JOIN folio_inventory.holdings_record hr on hr.instanceid = ins.id
    JOIN folio_inventory.item it on it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanentlocationid
    join folio_inventory.loclibrary__t ll on ll.id = hl.library_id
    JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    JOIN folio_inventory.material_type__t m on m.id = jsonb_extract_path_text(it.jsonb, 'materialTypeId')::uuid
    LEFT JOIN loans on loans.item_id = it.id
    WHERE
        (item_library = 'All' OR ll.name = item_library)       
        AND CASE
            WHEN asset_type = 'All' 
                THEN (insc.name = 'Calculator' AND m.name = 'SEM-ITEM') 
                OR (insc.name IN ('Laptop', 'Hotspot') AND m.name = 'SEMEXTEND-ITEM')
            WHEN asset_type = 'Calculator' 
                THEN insc.name = asset_type AND m.name = 'SEM-ITEM'
            ELSE 
                insc.name = asset_type AND m.name = 'SEMEXTEND-ITEM'
        END
    ORDER BY
        jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;