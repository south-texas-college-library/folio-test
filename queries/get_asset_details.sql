--metadb:function get_asset_details

DROP FUNCTION IF EXISTS get_asset_details;

CREATE FUNCTION get_asset_details(
    asset_type text DEFAULT NULL,
    campus_location text DEFAULT NULL
)
RETURNS TABLE(
    "A - Asset ID" TEXT,
    "a - Type" TEXT,
    "B - Student A#" TEXT,
    "C - Name" TEXT,
    "D - Phone Number" TEXT,
	"E - Email" TEXT,
    "F - Staff Notes" TEXT,
    "G - Campus Location" TEXT,
    "H - Status" TEXT
)
AS $$
    WITH loans AS (
        SELECT
            it.id AS item_id,
            jsonb_extract_path_text(u.jsonb, 'barcode') AS user_barcode,
            NULLIF(CONCAT(jsonb_extract_path_text(u.jsonb, 'personal', 'firstName'), ' ', jsonb_extract_path_text(u.jsonb, 'personal', 'lastName')), ' ') AS full_name,
            jsonb_extract_path_text(u.jsonb, 'personal', 'phone') AS phone,
            jsonb_extract_path_text(u.jsonb, 'personal', 'email') AS email
        FROM folio_inventory.instance ins
        JOIN folio_inventory.holdings_record hr ON hr.instanceid = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
        JOIN folio_circulation.loan l ON jsonb_extract_path_text(l.jsonb, 'itemId')::uuid = it.id
        JOIN folio_users.users u ON u.id = jsonb_extract_path_text(l.jsonb, 'userId')::uuid
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    )
    SELECT
        jsonb_extract_path_text(it.jsonb, 'barcode') as "Asset ID",
        insc.name,
        loans.user_barcode as "Student A#",
        loans.full_name as "Name",
        loans.phone as "Phone Number",
        loans.email as "Email",
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g'), '') as "Staff Notes",
        hl.name as "Campus Location",
        jsonb_extract_path_text(it.jsonb, 'status', 'name') as "Status"
    FROM folio_inventory.instance ins
    JOIN folio_inventory.holdings_record hr on hr.instanceid = ins.id
    JOIN folio_inventory.item it on it.holdingsrecordid = hr.id
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanentlocationid
    JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
    JOIN folio_inventory.material_type__t m on m.id = jsonb_extract_path_text(it.jsonb, 'materialTypeId')::uuid
    LEFT JOIN loans on loans.item_id = it.id
    WHERE
        (campus_location = 'All' OR hl.name = campus_location)       
        AND CASE
            WHEN asset_type = 'All' 
                THEN (insc.name = 'Calculator' AND m.name = 'SEM-ITEM') 
                OR (insc.name IN ('Laptop', 'Hotspot') AND m.name = 'SEMEXTEND-ITEM')
            WHEN asset_type = 'Calculator' 
                THEN m.name = 'SEM-ITEM'
            ELSE 
                m.name = 'SEMEXTEND-ITEM'
        END
    ORDER BY
        jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;