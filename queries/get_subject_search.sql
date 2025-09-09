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
SELECT
	folio_derived.items_holdings_instances.title AS title,
    folio_derived.locations_libraries.campus_name as campus,
    folio_derived.instance_contributors.contributor_name as author,
    folio_derived.items_holdings_instances.call_number as call_number,
    folio_derived.items_holdings_instances.barcode as item_barcode,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Price') AS price,
    folio_derived.items_holdings_instances.material_type_name as item_type,
    folio_derived.items_holdings_instances.cataloged_date as date_created,
	(SELECT MAX(val[1]::text) from regexp_matches(folio_derived.instance_publication.date_of_publication, '\d{4}', 'g') as val) as publication_date,
    string_agg(DISTINCT split_part(folio_derived.instance_identifiers.identifier, ' : ', 1), ', ') AS identifier,
    folio_derived.holdings_ext.permanent_location_name as home_location,
    folio_derived.item_ext.effective_location_name as current_location,
    string_agg(DISTINCT jsonb_extract_path_text(folio_derived.instance_subjects.subjects::jsonb, 'value'), ', ') AS subjects,
    folio_derived.loans_renewal_count.num_renewals as total_renewals,
    (SELECT COUNT(*) FROM folio_derived.loans_items WHERE folio_derived.loans_items.item_id = folio_derived.items_holdings_instances.item_id) AS total_checkouts,
    folio_derived.instance_statistical_codes.statistical_code_name as content,
    folio_derived.instance_publication.publisher as publisher,
    folio_derived.item_statistical_codes.statistical_code_name as subtype
FROM 
    folio_derived.items_holdings_instances
    LEFT JOIN folio_derived.holdings_ext ON folio_derived.holdings_ext.holdings_id = folio_derived.items_holdings_instances.holdings_id
    LEFT JOIN folio_derived.item_ext ON folio_derived.item_ext.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.instance_contributors on folio_derived.instance_contributors.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.instance_publication ON folio_derived.instance_publication.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.instance_identifiers ON folio_derived.instance_identifiers.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.instance_statistical_codes ON folio_derived.instance_statistical_codes.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.instance_subjects ON folio_derived.instance_subjects.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.item_notes ON folio_derived.item_notes.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.item_statistical_codes on folio_derived.item_statistical_codes.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.loans_renewal_count ON folio_derived.loans_renewal_count.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.locations_libraries ON folio_derived.locations_libraries.location_id = folio_derived.item_ext.effective_location_id
WHERE 
    to_tsvector(replace(jsonb_extract_path_text(folio_derived.instance_subjects.subjects::jsonb, 'value'), '--', ' ')) @@ websearch_to_tsquery(subject)
    AND folio_derived.item_notes.note_type_name = 'Price'
    AND (folio_derived.instance_contributors.contributor_ordinality = 1 OR folio_derived.instance_contributors.contributor_ordinality IS NULL)
    AND (folio_derived.instance_publication.publication_ordinality = 1 OR folio_derived.instance_publication.publication_ordinality IS NULL)
GROUP BY
    folio_derived.holdings_ext.permanent_location_name,
    folio_derived.instance_contributors.contributor_name,
    folio_derived.instance_publication.date_of_publication,
    folio_derived.instance_publication.publication_ordinality,
    folio_derived.instance_publication.publication_place,
    folio_derived.instance_publication.publication_role,
    folio_derived.instance_publication.publisher,
    folio_derived.instance_statistical_codes.statistical_code_name,
    folio_derived.item_ext.effective_location_name,
    folio_derived.item_notes.note,
    folio_derived.item_statistical_codes.statistical_code_name,
    folio_derived.items_holdings_instances.barcode,
    folio_derived.items_holdings_instances.call_number,
    folio_derived.items_holdings_instances.cataloged_date,
    folio_derived.items_holdings_instances.instance_id,
    folio_derived.items_holdings_instances.item_id,
    folio_derived.items_holdings_instances.material_type_name,
    folio_derived.items_holdings_instances.title,
    folio_derived.loans_renewal_count.num_renewals,
    folio_derived.locations_libraries.campus_name
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;