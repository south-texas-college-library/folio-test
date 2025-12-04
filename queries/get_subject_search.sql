--metadb:function get_subject_search

DROP FUNCTION IF EXISTS get_subject_search;

CREATE FUNCTION get_subject_search(
    subject text DEFAULT NULL
) 
RETURNS TABLE(
    "A - Title" TEXT,
    "B - Campus" TEXT,
    "C - Author" TEXT,
    "D - call_number" TEXT,
    "E - Item Barcode" TEXT,
    "F - Item Price" TEXT,
    "G - Item Type" TEXT,
    "H - Date Created" TEXT,
    "I - Publication Date" TEXT,
    "J - ISBN" TEXT,
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
		l.renewal_count AS renewals
	FROM folio_circulation.loan__t l
	LEFT JOIN folio_inventory.item i ON i.id = l.item_id
	GROUP BY 
		l.item_id, 
		l.renewal_count
)
SELECT
    ins.jsonb ->> 'title' AS title,
    lc.name AS campus,
    jsonb_path_query_first(ins.jsonb, '$.contributors[*].name') #>> '{}' AS author,	
    hr.call_number AS call_number,
    it.jsonb ->> 'barcode' AS barcode,
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), '') AS price,
    mt.name AS material_type,
    ins.jsonb ->> 'catalogedDate' AS date_created,
    GREATEST(ins.jsonb -> 'publicationPeriod' ->> 'start', ins.jsonb -> 'publicationPeriod' ->> 'end') AS publication_date,
	NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', ' :.*?\$\d+\.\d{2}', '', 'g'), '[\[\]"]', '', 'g'), '') AS identifiers,
    hl.name AS home_location,
    il.name AS current_location,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[\[\]"]', '', 'g') AS subjects,
    COALESCE(loans.checkouts, 0) AS checkouts,
    COALESCE(loans.renewals, 0) AS renewals,
	jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' AS publisher,
    insc.jsonb ->> 'name' AS subtype,
	itsc.jsonb ->> 'name' AS fund
FROM
	folio_inventory.instance ins
	LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
	LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
	LEFT JOIN folio_inventory.loan_type__t plt ON plt.id = it.permanentloantypeid
	LEFT JOIN folio_inventory.loan_type__t tlt ON tlt.id = it.temporaryloantypeid
	LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	LEFT JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
	LEFT JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
	LEFT JOIN folio_inventory.loccampus__t lc ON lc.id = hl.campus_id
	LEFT JOIN folio_inventory.statistical_code insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
	LEFT JOIN folio_inventory.statistical_code itsc ON itsc.id = (jsonb_path_query_first(it.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
	LEFT JOIN loans ON loans.item_id = it.id
WHERE 
	to_tsvector(REPLACE(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[\[\]"]', '', 'g'), '--', ' ')) @@ websearch_to_tsquery(subject)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;