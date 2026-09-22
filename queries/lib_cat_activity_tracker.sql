--metadb:function lib_cat_activity_tracker

DROP FUNCTION IF EXISTS lib_cat_activity_tracker;

CREATE FUNCTION lib_cat_activity_tracker(
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    cataloger text,
    instance_added numeric,
    instance_updated numeric,
    item_added numeric,
    item_updated numeric,
    item_withdrawn numeric,
    marc_added numeric,
    marc_modified numeric,
    marc_updated numeric,
    marc_deleted numeric,
)
AS $$
WITH catalogers (username, cataloger) AS (
    VALUES
        ('ahern267', 'boomer'),
        ('treyna1',  'helo'),
        ('mtorre43', 'apollo'),
        ('marjona4', 'starbuck'),
        (NULL,        'husker')
),
instance_added AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        COUNT(jsonb_extract_path_text(i.jsonb, 'hrid')) AS inst_added
    FROM folio_inventory.instance__ i
    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_path_query_first(pu.jsonb,'$.permissions[*]') #>> '{}' = '07e78044-2804-496e-a3e7-074f557dd361'
       AND jsonb_extract_path_text(pu.jsonb, 'userId')::uuid = jsonb_extract_path_text(i.jsonb, 'metadata', 'createdByUserId')::uuid
    LEFT JOIN folio_users.users__t created_by
        ON created_by.id = jsonb_extract_path_text(pu.jsonb, 'userId')::uuid
    LEFT JOIN catalogers c
        ON c.username = created_by.username
    CROSS JOIN report_dates d
    WHERE jsonb_extract_path_text(i.jsonb, 'hrid') !~ '^(SE|L|RSV|T)' AND jsonb_extract_path_text(i.jsonb, 'metadata', 'createdDate')::date
          BETWEEN d.start_date AND d.end_date
    GROUP BY COALESCE(c.cataloger, 'husker')
),
instance_updated AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        COUNT(jsonb_extract_path_text(i.jsonb, 'hrid')) AS inst_updated
    FROM folio_inventory.instance__ i
    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_path_query_first(pu.jsonb, '$.permissions[*]') #>> '{}' = '07e78044-2804-496e-a3e7-074f557dd361'
       AND jsonb_extract_path_text(pu.jsonb, 'userId')::uuid =
           jsonb_extract_path_text(i.jsonb, 'metadata', 'updatedByUserId')::uuid
    LEFT JOIN folio_users.users__t updated_by
        ON updated_by.id = jsonb_extract_path_text(pu.jsonb, 'userId')::uuid
    LEFT JOIN catalogers c
        ON c.username = updated_by.username
    CROSS JOIN report_dates d
    WHERE jsonb_extract_path_text(i.jsonb, 'hrid') !~ '^(SE|L|RSV|T)' AND jsonb_extract_path_text(i.jsonb, 'metadata', 'updatedDate')::date
          BETWEEN d.start_date
              AND d.end_date
    GROUP BY COALESCE(c.cataloger, 'husker')
),
item_added AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        COUNT(jsonb_extract_path_text(i.jsonb, 'barcode')) AS item_added
    FROM folio_inventory.item__ i
    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_path_query_first(pu.jsonb, '$.permissions[*]') #>> '{}' = '07e78044-2804-496e-a3e7-074f557dd361' 
        AND jsonb_extract_path_text(pu.jsonb,'userId')::uuid =
           jsonb_extract_path_text(i.jsonb,'metadata', 'createdByUserId')::uuid
    LEFT JOIN folio_users.users__t created_by
        ON created_by.id = jsonb_extract_path_text(pu.jsonb, 'userId')::uuid
    LEFT JOIN catalogers c
        ON c.username = created_by.username
    CROSS JOIN report_dates d
    WHERE jsonb_extract_path_text(i.jsonb, 'barcode') !~ '^(SE|L|RSV|T)' AND jsonb_extract_path_text(i.jsonb, 'metadata', 'createdDate')::date
          BETWEEN d.start_date
              AND d.end_date
    GROUP BY COALESCE(c.cataloger, 'husker')
),
item_updated AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        COUNT(jsonb_extract_path_text(i.jsonb, 'barcode')) AS item_updated
    FROM folio_inventory.item__ i
    LEFT JOIN folio_permissions.permissions_users pu
        ON jsonb_path_query_first(pu.jsonb,'$.permissions[*]') #>> '{}' = '07e78044-2804-496e-a3e7-074f557dd361'
       AND jsonb_extract_path_text(pu.jsonb, 'userId')::uuid = jsonb_extract_path_text(i.jsonb, 'metadata', 'updatedByUserId')::uuid
    LEFT JOIN folio_users.users__t updated_by
        ON updated_by.id = jsonb_extract_path_text(pu.jsonb, 'userId')::uuid
    LEFT JOIN catalogers c
        ON c.username = updated_by.username
    CROSS JOIN report_dates d
    WHERE jsonb_extract_path_text(i.jsonb, 'barcode') !~ '^(SE|L|RSV|T)' AND jsonb_extract_path_text(i.jsonb, 'metadata', 'updatedDate')::date
          BETWEEN d.start_date
              AND d.end_date
    GROUP BY COALESCE(c.cataloger, 'husker')
),
item_withdrawn AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        COUNT(jsonb_extract_path_text(i.jsonb, 'barcode')) AS item_withdrawn
    FROM folio_inventory.item__ i
    LEFT JOIN folio_users.users__t updated_by
        ON updated_by.id = jsonb_extract_path_text(i.jsonb, 'metadata', 'updatedByUserId')::uuid
    LEFT JOIN catalogers c
        ON c.username = updated_by.username
    CROSS JOIN report_dates d
    WHERE jsonb_extract_path_text(i.jsonb, 'status', 'name') = 'Withdrawn' AND jsonb_extract_path_text(i.jsonb, 'status', 'date' )::date
          BETWEEN d.start_date
              AND d.end_date
    GROUP BY COALESCE(c.cataloger, 'husker')
),
audit_data AS (
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p0_2026_q2__
    WHERE action = 'UPDATED'/*
    UNION all
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p0_2026_q3__
    WHERE action = 'UPDATED'*/
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p1_2026_q2__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p1_2026_q3__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p2_2026_q2__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p2_2026_q3__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p3_2026_q2__
    WHERE action = 'UPDATED'/*
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p3_2026_q3__
    WHERE action = 'UPDATED'*/
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p4_2026_q2__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p4_2026_q3__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p5_2026_q2__
    WHERE action = 'UPDATED'/*
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p5_2026_q3__
    WHERE action = 'UPDATED'*/
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p6_2026_q2__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p6_2026_q3__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p7_2026_q2__
    WHERE action = 'UPDATED'
    UNION ALL
    SELECT user_id, action, diff, event_date
    FROM folio_audit.marc_bib_audit_p7_2026_q3__
    WHERE action = 'UPDATED'
),
field_changes AS (
    SELECT
        ad.user_id,
        ad.event_date,
        fc ->> 'fieldName'  AS field_name,
        fc ->> 'fullPath'   AS full_path,
        fc ->> 'changeType' AS change_type
    FROM audit_data ad
    CROSS JOIN report_dates d
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(ad.diff::jsonb -> 'fieldChanges', '[]'::jsonb)) AS fc
    WHERE ad.event_date::date BETWEEN d.start_date AND d.end_date
),
marc_change_counts AS (
    SELECT
        COALESCE(c.cataloger, 'husker') AS cataloger,
        fc.change_type,
        COUNT(*) AS change_count
    FROM field_changes fc
    LEFT JOIN folio_users.users__t audit_user
        ON audit_user.id = fc.user_id
    LEFT JOIN catalogers c
        ON c.username = audit_user.username
    WHERE fc.change_type IS NOT NULL
    GROUP BY
        COALESCE(c.cataloger, 'husker'), fc.change_type
),
marc_summary AS (
    SELECT
        cataloger,
        COALESCE(SUM(change_count) FILTER (WHERE change_type = 'ADDED'), 0) AS marc_added,
        COALESCE(SUM(change_count) FILTER (WHERE change_type = 'DELETED'), 0) AS marc_deleted,
        COALESCE(SUM(change_count) FILTER (WHERE change_type = 'MODIFIED'), 0) AS marc_modified,
        COALESCE(SUM(change_count) FILTER (WHERE change_type = 'UPDATED'), 0) AS marc_updated,
        SUM(change_count) AS total_marc_field_changes
    FROM marc_change_counts
    GROUP BY cataloger
)
SELECT
    c.cataloger,
    --c.username,
    COALESCE(ia.inst_added, 0)     AS "Instance Added",
    COALESCE(iu.inst_updated, 0)   AS "Instance Updated",
    COALESCE(ita.item_added, 0)    AS "Item Added",
    COALESCE(itu.item_updated, 0)  AS "Item Updated",
    COALESCE(iw.item_withdrawn, 0) AS "Item Withdrawn",
    COALESCE(ms.marc_added, 0)    AS "MARC Added",
    COALESCE(ms.marc_deleted, 0)  AS "MARC Deleted",
    COALESCE(ms.marc_modified, 0) AS "MARC Modified",
    COALESCE(ms.marc_updated, 0)  AS "MARC Updated"/*,
    COALESCE(ms.total_marc_field_changes, 0) AS "Total MARC Field Changes"*/
FROM catalogers c
LEFT JOIN instance_added ia
    ON ia.cataloger = c.cataloger
LEFT JOIN instance_updated iu
    ON iu.cataloger = c.cataloger
LEFT JOIN item_added ita
    ON ita.cataloger = c.cataloger
LEFT JOIN item_updated itu
    ON itu.cataloger = c.cataloger
LEFT JOIN item_withdrawn iw
    ON iw.cataloger = c.cataloger
LEFT JOIN marc_summary ms
    ON ms.cataloger = c.cataloger
ORDER BY
    CASE c.cataloger
        WHEN 'boomer'   THEN 1
        WHEN 'helo'     THEN 2
        WHEN 'apollo'   THEN 3
        WHEN 'starbuck' THEN 4
        WHEN 'husker'   THEN 5
        ELSE 6
    END;
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;