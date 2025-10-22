--metadb:function get_subject_search

DROP FUNCTION IF EXISTS get_subject_search;

CREATE FUNCTION get_subject_search(
    subject text DEFAULT NULL
) 
RETURNS TABLE(
    title TEXT,
    campus TEXT,
    author TEXT,
    call_number TEXT,
    item_barcode TEXT,
    price TEXT,
    item_type TEXT,
    date_created TEXT,
    publication_date TEXT,
    isbn TEXT,
    home_location TEXT,
    current_location TEXT,
    subjects TEXT,
    total_renewals INTEGER,
    total_checkouts INTEGER,
    content TEXT,
    publisher TEXT,
    subtype TEXT
)
AS $$
WITH fc AS (
	SELECT
		ins.id AS id,
		c.jsonb ->> 'name' AS name
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL jsonb_array_elements(ins.jsonb -> 'contributors') WITH ORDINALITY AS c (jsonb)
	WHERE 
		c.ordinality = 1
),
fi AS (
	SELECT 
		id,
		STRING_AGG(DISTINCT SPLIT_PART(i.jsonb ->> 'value', ' : ', 1), ', ') AS identifiers
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'identifiers') AS i
	GROUP BY
		ins.id
),
fl AS (
	SELECT
		l.item_id AS id,
		COUNT(l.id) AS checkouts,
		l.renewal_count AS renewals
	FROM folio_circulation.loan__t l
	LEFT JOIN folio_inventory.item i ON i.id = l.item_id
	GROUP BY 
		l.item_id, 
		l.renewal_count
),
fn AS (
	SELECT 
		it.id AS id,
		n ->> 'note' AS price
	FROM folio_inventory.item it
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(it.jsonb -> 'notes') AS n
	LEFT JOIN folio_inventory.item_note_type nt ON nt.id = (n ->> 'itemNoteTypeId')::uuid
	WHERE 
		nt.jsonb ->> 'name' = 'Price'
),
fp AS (
	SELECT
		ins.id AS id,
		GREATEST(ins.jsonb -> 'publicationPeriod' ->> 'start', ins.jsonb -> 'publicationPeriod' ->> 'end') AS pub_date,
		p.jsonb ->> 'publisher' AS publisher
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'publication') WITH ORDINALITY AS p (jsonb)
	WHERE 
		p.ordinality = 1
),
fs AS (
	SELECT
		ins.id AS id,
		STRING_AGG(s.jsonb ->> 'value', ', ') AS subjects
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'subjects') AS s
	GROUP BY 
		ins.id
),
sc AS (
	SELECT
		ins.id AS id,
		it.id AS item_id,
		insc.jsonb ->> 'name' AS instance_code,
		itsc.jsonb ->> 'name' AS item_code
	FROM 
		folio_inventory.instance ins
		LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
		LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
		CROSS JOIN LATERAL ROWS FROM (
			JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'statisticalCodeIds'), 
			JSONB_ARRAY_ELEMENTS(it.jsonb -> 'statisticalCodeIds')
		) x(y, z)
		--CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'statisticalCodeIds') AS finsc
		--CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(it.jsonb -> 'statisticalCodeIds') AS fitsc
		LEFT JOIN folio_inventory.statistical_code insc ON insc.id = (y #>> '{}')::uuid
		LEFT JOIN folio_inventory.statistical_code itsc ON itsc.id = (z #>> '{}')::uuid
)
SELECT
    ins.jsonb ->> 'title' AS title,
    lc.name AS campus,
    fc.name AS author,
    hr.call_number AS call_number,
    it.jsonb ->> 'barcode' AS barcode,
    fn.price AS price,
    mt.name AS material_type,
    ins.jsonb ->> 'catalogedDate' AS date_created,
    fp.pub_date AS publication_date,
    fi.identifiers AS identifiers,
    hl.name AS home_location,
    il.name AS current_location,
    fs.subjects AS subjects,
    COALESCE(fl.checkouts, 0) AS checkouts,
    COALESCE(fl.renewals, 0) AS renewals,
    fp.publisher AS publisher,
    sc.instance_code AS subtype,
    sc.item_code AS fund  
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
	LEFT JOIN fc ON fc.id = ins.id
	LEFT JOIN fi ON fi.id = ins.id
	LEFT JOIN fl ON fl.id = it.id
	LEFT JOIN fn ON fn.id = it.id
	LEFT JOIN fp ON fp.id = ins.id
	LEFT JOIN fs ON fs.id = ins.id
	LEFT JOIN sc ON sc.id = it.id
WHERE 
	to_tsvector(replace(fs.subjects, '--', ' ')) @@ websearch_to_tsquery(subject)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;