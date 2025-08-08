\ o PERFORMANCEOutput
/************************************************************************************************************
 
 load all prop29 values and pvids into temp
 
 ************************************************************************************************************/
TRUNCATE TABLE azahn.SF_CBP_TEMP1;

INSERT INTO
    azahn.SF_CBP_TEMP1(
        SELECT
            split_part(pv.SPONSOR_PROGRAM_NM, '_', 1) AS SPONSOR_CLIENT_NM,
            split_part(pv.SPONSOR_PROGRAM_NM, '_', 2) AS SPONSOR_BRAND_NM,
            pv.SPONSOR_PROGRAM_NM,
            min(pv.PVID) AS MIN_PVID,
            max(pv.PVID) AS MAX_PVID
        FROM
            omniture_new.core_page_view pv
            INNER JOIN omniture_new.Global_Visit vis ON pv.visitor_ID = vis.visitor_Id
            AND pv.visit_num = vis.VISIT_NUM
        WHERE
            pv.page_View_dtm BETWEEN :VStartTime
            AND :VEndTime
            AND vis.initial_page_View_dtm BETWEEN timestampadd(DAY, -1, :VStartTime)
            AND timestampadd(DAY, 1, :VEndTime) -- updated 1/14/20 to expand global_visit date filter
            AND vis.COUNTRY_NM = 'usa'
            AND nvl(pv.sponsor_program_nm, 'ntc') <> 'ntc'
            AND pv.site_nm IN(
                'core',
                'rxlist',
                'emedicinehealth',
                'medicinenet',
                'onhealth'
            )
            AND pv.PAGE_EVENT_CD = 0
            AND pv.pvid IS NOT NULL
        GROUP BY
            split_part(pv.SPONSOR_PROGRAM_NM, '_', 1),
            split_part(pv.SPONSOR_PROGRAM_NM, '_', 2),
            pv.SPONSOR_PROGRAM_NM
    );

COMMIT;

/************************************************************************************************************
 
 join cbp temp to dfp get advertiser and SF numbers
 
 ************************************************************************************************************/
INSERT INTO
    azahn.SF_CBP_LOOKUP(
        ADVERTISER,
        SFNUMBER,
        SPONSOR_CLIENT_NM,
        SPONSOR_BRAND_NM,
        SPONSOR_PROGRAM_NM,
        ORDERS_START_DATETIME,
        ORDERS_END_DATETIME
    )(
        SELECT
            ad.COMPANY_NAME AS ADVERTISER,
            ad.SFNUMBER,
            t.SPONSOR_CLIENT_NM,
            t.SPONSOR_BRAND_NM,
            t.SPONSOR_PROGRAM_NM,
            min(ad.ORDERS_START_DATETIME) AS ORDERS_START_DATETIME,
            max(ad.ORDERS_END_DATETIME) AS ORDERS_END_DATETIME
        FROM
            azahn.SF_CBP_TEMP1 t
            INNER JOIN dfp.impression imp ON t.min_pvid = imp.pvid
            INNER JOIN azahn.vw_dfp_admanager ad ON imp.lineitemid = ad.lineitem_id
            AND imp.creativeid = ad.creative_id
            LEFT JOIN azahn.SF_CBP_LOOKUP t2 ON ad.COMPANY_NAME = t2.ADVERTISER
            AND ad.SFNUMBER = t2.SFNUMBER
            AND t.SPONSOR_CLIENT_NM = t2.SPONSOR_CLIENT_NM
            AND t.SPONSOR_BRAND_NM = t2.SPONSOR_BRAND_NM
            AND t.SPONSOR_PROGRAM_NM = t2.SPONSOR_PROGRAM_NM
        WHERE
            imp.eventtime BETWEEN :VStartTime
            AND :VEndTime
            AND ad.company_name NOT ilike '%WebMD Ad Server Master%'
            AND ad.company_name NOT ilike '%Pubmatic%'
            AND ad.company_name NOT ilike '%WebMD Consumer Research%'
            AND ad.COMPANY_NAME NOT IN('House - Consumer')
            AND t2.sponsor_program_nm IS NULL
        GROUP BY
            ad.COMPANY_NAME,
            ad.SFNUMBER,
            t.SPONSOR_CLIENT_NM,
            t.SPONSOR_BRAND_NM,
            t.SPONSOR_PROGRAM_NM
    );

COMMIT;

/************************************************************************************************************
 
 summarize DFP data
 
 ************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    DFP_PVID_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PERFORMANCE_DFP_PVID (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

------------------------------------------------------ TRUNCATE ------------------------------------------------------
\ qecho TRUNCATE DFP_PVID TRUNCATE TABLE hliang.PERFORMANCE_DFP_PVID;

------------------------------------------------------ INSERT ------------------------------------------------------
\ qecho
INSERT
    DFP_PVID
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_DFP_PVID(
        SELECT
            imp.pvid,
            trunc(imp.eventtime) AS page_View_dt,
            imp.EVENTTIME,
            left(imp.PAGE_URI, 1000) AS PAGE_URI,
            imp.PRIMARY_TOPIC,
            imp.POS,
            imp.BROWSERID,
            (
                CASE
                    WHEN geo.name = 'United States' THEN 'US'
                    ELSE 'xUS'
                END
            ) AS Country,
            ad.company_name,
            sf.PROGRAM_NAME,
            ad.lineitem_id,
            ad.orders_id,
            ad.orders_name,
            NULL AS product_name,
            ad.creative_name,
            ad.adsize,
            ad.creative_id,
            ad.SFNUMBER,
            (
                CASE
                    WHEN imp.DEVICECATEGORY ilike 'Mobile%' THEN 'Mobile Phone'
                    WHEN imp.DEVICECATEGORY ilike '%tablet%' THEN 'Tablet'
                    WHEN imp.DEVICECATEGORY ilike 'Desktop' THEN 'PC'
                    ELSE 'Other'
                END
            ) AS Device_Type_nm,
            ----mg.media_group,
            ----ad.media_group, --changed 1/18/18
            --abl.media_group, --changed 7/8/19
            'Media' AS Media_Type,
            2 AS Type_Priority,
            ----ad.media_group_priority as Group_Priority, --changed 1/18/18
            --abl.media_group_priority as Group_Priority, --changed 7/8/19
            ad.media_group AS lf_media_group,
            nvl(ad.productfamily, 'UNKNOWN') || ' - ' || nvl(ad.ad_product, 'UNKNOWN') AS media_group,
            --p1.media_group as khan_media_group_curr,
            nvl(p1.priority, 99) AS khan_media_priority_curr,
            count(*) AS Impressions,
            sum(
                CASE
                    WHEN imp.ActiveViewEligiblempression = 'Y' THEN 1
                    ELSE 0
                END
            ) AS Eligible,
            sum(
                CASE
                    WHEN imp.ActiveViewEligiblempression = 'Y'
                    AND nvl(imp.measurableimpression, 'Y') = 'Y' THEN 1
                    ELSE 0
                END
            ) AS Measurable,
            sum(
                CASE
                    WHEN imp.viewableimpression = 'Y' THEN 1
                    ELSE 0
                END
            ) AS Viewable,
            sum(imp.CLICK_COUNT) AS clicks,
            litar.AUDIENCE_TARGET --added 1/18/18
,
            CASE
                WHEN lpos.display = 1 THEN 1
                ELSE 0
            END display
        FROM
            dfp.impression imp
            INNER JOIN lfelix.dfp_admanager ad --updated Sept 2023
            ON imp.lineitemid = ad.LINEITEM_ID
            AND imp.creativeid = ad.CREATIVE_ID
            INNER JOIN dfp.lkup_position lpos ON imp.pos = lpos.pos -- display ads only, added 1/9/2020
            /***updated Sept 2023 ***/
            LEFT JOIN khan.adbook_media_group_priority p1 ON ad.productfamily || ' - ' || ad.ad_product = p1.media_group
            LEFT JOIN dfp.geo_targets geo ON imp.COUNTRYID = geo.id
            AND geo.TYPE = 'COUNTRY'
            LEFT JOIN (
                SELECT
                    litar.object_id AS LINEITEM_ID,
                    max(
                        CASE
                            WHEN litar.targeting_field = 'dmp' THEN lo.audience_name
                            ELSE NULL
                        END
                    ) AUDIENCE_TARGET --added 1/18/18
                FROM
                    azahn.vw_dfp_custom_targeting litar
                    LEFT JOIN azahn.vw_Lotame_Audiences lo ON litar.Operator_Nm = lo.dmp_value :: varchar
                    AND lo.AUDIENCE_NAME NOT ilike 'WebMD - Test%'
                WHERE
                    litar.table_name = 'lineitem'
                    AND litar.Operator = 'IS'
                    AND litar.Operator_Type = 'EXACT'
                    AND litar.targeting_field = 'dmp' -- in('dsf','dsy','leaf','dmp','fif','fipt','fis','uri')
                GROUP BY
                    litar.object_id
            ) litar ON ad.lineitem_id = litar.LINEITEM_ID
            INNER JOIN (
                SELECT
                    DISTINCT sf.SF_NUMBER,
                    sf.PROGRAM_NAME
                FROM
                    azahn.PERFORMANCE_SFNUMBERS sf
            ) sf ON ad.SFNumber = sf.SF_NUMBER
        WHERE
            imp.eventtime BETWEEN :VStartTime
            AND :VEndTime
            /***updated filters in Dec 2023 ***/
            AND (
                display = 1
                OR lpos.pos IN (923, 5000)
            )
            AND include_consbi = 1
        GROUP BY
            imp.pvid,
            trunc(imp.eventtime),
            imp.EVENTTIME,
            left(imp.PAGE_URI, 1000),
            imp.PRIMARY_TOPIC,
            imp.POS,
            imp.BROWSERID,
            (
                CASE
                    WHEN geo.name = 'United States' THEN 'US'
                    ELSE 'xUS'
                END
            ),
            ad.company_name,
            sf.PROGRAM_NAME,
            ad.lineitem_id,
            ad.orders_id,
            ad.orders_name,
            --ad.product_name,
            ad.creative_name,
            ad.adsize,
            ad.creative_id,
            ad.SFNUMBER,
            (
                CASE
                    WHEN imp.DEVICECATEGORY ilike 'Mobile%' THEN 'Mobile Phone'
                    WHEN imp.DEVICECATEGORY ilike '%tablet%' THEN 'Tablet'
                    WHEN imp.DEVICECATEGORY ilike 'Desktop' THEN 'PC'
                    ELSE 'Other'
                END
            ),
            --abl.media_group, --changed 7/8/19
            --abl.media_group_priority, --changed 7/8/19
            ad.media_group,
            nvl(ad.productfamily, 'UNKNOWN') || ' - ' || nvl(ad.ad_product, 'UNKNOWN'),
            --p1.media_group,
            nvl(p1.priority, 99),
            litar.AUDIENCE_TARGET --mg.AUDIENCE_TARGET--added 9/16/16
,
            CASE
                WHEN lpos.display = 1 THEN 1
                ELSE 0
            END
    );

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    DFP_PVID_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PERFORMANCE_DFP_PVID (New)' AS name,
            PROGRAM_NAME,
            trunc(eventtime) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_DFP_PVID --where trunc(eventtime) = :VStartTime::timestamp  ---to remove
        GROUP BY
            trunc(eventtime),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_DFP_PVID');

/************************************************************************************************************
 
 summarize OMNITURE data
 
 ************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    OMNI_PVID_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PERFORMANCE_OMNI_PVID (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

-------------------------------------------------------- TRUNCATE ------------------------------------------------------
\ qecho TRUNCATE OMNI_PVID TRUNCATE TABLE hliang.PERFORMANCE_OMNI_PVID;

-------------------------------------------------------- INSERT ------------------------------------------------------
INSERT
    /*+direct*/
    INTO hliang.PERFORMANCE_OMNI_PVID
SELECT
    pv.pvid,
    pv.visitor_id,
    pv.visit_num,
    vis.page_view_num,
    pv.page_View_dtm,
    vis.Country,
    vis.Browser_id,
    vis.OS_ID,
    pv.subject_cd,
    left(pv.page_nm, 1000) AS page_nm,
    pv.mobile_optimized_cd,
    vis.device_type_nm,
    pv.site_nm,
    pv.TRAFFIC_SOURCE,
    pv.SPONSOR_CLIENT_NM,
    pv.SPONSOR_BRAND_NM,
    pv.SPONSOR_PROGRAM_NM,
    vis.Page_Views,
    page_View_dt ----added to account for running multiple days at a time
FROM
    omniture_new.core_page_View pv
    INNER JOIN (
        SELECT
            trunc(page_View_dtm) AS page_View_dt,
            ----added to account for running multiple days at a time
            pv.pvid,
            pv.visitor_id,
            pv.visit_num,
            (
                CASE
                    WHEN vis.country_nm = 'usa' THEN 'US'
                    ELSE 'xUS'
                END
            ) AS Country,
            vis.Browser_id,
            vis.OS_ID,
            vis.device_type_nm,
            min(pv.page_View_num) AS page_view_num,
            sum(
                CASE
                    WHEN pv.page_Event_cd = 0 THEN 1
                    ELSE 0
                END
            ) AS Page_Views
        FROM
            omniture_new.core_page_View pv
            INNER JOIN omniture_new.global_visit vis ON pv.visitor_id = vis.visitor_ID
            AND pv.visit_num = vis.visit_num
        WHERE
            pv.page_View_dtm BETWEEN :VStartTime
            AND :VEndTime
            AND vis.initial_page_View_dtm BETWEEN timestampadd(DAY, -1, :VStartTime)
            AND timestampadd(DAY, 1, :VEndTime) -- updated 1/14/20 to expand global_visit date filter
            AND pv.page_Event_cd = 0
            AND pv.site_nm IN(
                'core',
                'rxlist',
                'emedicinehealth',
                'medicinenet',
                'onhealth'
            )
        GROUP BY
            trunc(page_View_dtm),
            ----added to account for running multiple days at a time
            pv.pvid,
            pv.visitor_id,
            pv.visit_num,
            vis.device_type_nm,
            (
                CASE
                    WHEN vis.country_nm = 'usa' THEN 'US'
                    ELSE 'xUS'
                END
            ),
            vis.Browser_id,
            vis.OS_ID
    ) vis ON pv.visitor_ID = vis.VISITOR_ID
    AND pv.visit_num = vis.VISIT_NUM
    AND pv.page_View_num = vis.page_View_num
    AND pv.pvid = vis.pvid
    AND trunc(pv.page_View_dtm) = page_View_dt ----added to account for running multiple days at a time
WHERE
    pv.page_View_dtm BETWEEN :VStartTime
    AND :VEndTime
    AND pv.page_Event_cd = 0
    AND pv.site_nm IN(
        'core',
        'rxlist',
        'emedicinehealth',
        'medicinenet',
        'onhealth'
    );

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    OMNI_PVID_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PERFORMANCE_OMNI_PVID (New)' AS name,
            upper(sponsor_brand_nm) AS PROGRAM_NAME,
            trunc(page_view_dtm) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_OMNI_PVID
        WHERE
            trunc(page_view_dtm) = :VStartTime :: timestamp ---to remove
        GROUP BY
            trunc(page_view_dtm),
            upper(sponsor_brand_nm)
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_OMNI_PVID');

/************************************************************************************************************
 
 JOIN DFP and Omniture data insert into holding table
 
 ************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    HOLDING_IMPS_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PERFORMANCE_HOLDING_IMPS (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

------------------------------------------------------ INSERT ------------------------------------------------------
\ qecho
INSERT
    HOLDING_IMPS
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_HOLDING_all
SELECT
    OMNI.PVID,
    OMNI.VISITOR_ID,
    OMNI.VISIT_NUM,
    OMNI.PAGE_VIEW_NUM,
    OMNI.PAGE_VIEW_DTM,
    OMNI.COUNTRY,
    OMNI.BROWSER_ID,
    OMNI.OS_ID,
    OMNI.SUBJECT_CD,
    nvl(topic.TOPIC_NM, 'UNKNOWN') AS TOPIC_NM,
    left(OMNI.PAGE_NM, 1000) AS PAGE_NM,
    OMNI.MOBILE_OPTIMIZED_CD,
    OMNI.DEVICE_TYPE_NM,
    OMNI.SITE_NM,
    ref.TRAFFIC_SOURCE_CLASS,
    REF.TRAFFIC_SOURCE_GROUP,
    --case when omni.traffic_source = 'referring module' and omni.page_view_num = 1 then 'seo' else ref.traffic_source_class end as traffic_source_class, ---to account for first pv referring module
    --case when omni.traffic_source = 'referring module' and omni.page_view_num = 1 then 'direct' else ref.traffic_source_group end as traffic_source_group,---to account for first pv referring module
    OMNI.TRAFFIC_SOURCE,
    OMNI.SPONSOR_CLIENT_NM,
    OMNI.SPONSOR_BRAND_NM,
    OMNI.SPONSOR_PROGRAM_NM,
    DFP.company_name AS ADVERTISER,
    DFP.PROGRAM_NAME,
    DFP.LINEITEM_ID,
    DFP.ORDERS_ID,
    DFP.ORDERS_NAME,
    DFP.PRODUCT_NAME,
    DFP.CREATIVE_NAME,
    DFP.adsize AS ADSIZE,
    DFP.CREATIVE_ID,
    DFP.SFNUMBER,
    DFP.MEDIA_GROUP,
    DFP.MEDIA_TYPE,
    DFP.TYPE_PRIORITY,
    --DFP.GROUP_PRIORITY,
    dfp.khan_media_priority_curr AS GROUP_PRIORITY,
    DFP.IMPRESSIONS,
    DFP.Eligible AS ELIGIBLE_IMPS,
    DFP.Measurable AS MEASURABLE_IMPS,
    DFP.Viewable AS VIEWABLE_IMPS,
    DFP.CLICKS,
    0 AS EXCLUDE_IMP,
    nvl(dfp.AUDIENCE_TARGET, DFP.MEDIA_GROUP) AS AUDIENCE_TARGET --added 9/16/16
,
    dfp.lf_media_group --,dfp.khan_media_group_curr
,
    dfp.display
FROM
    hliang.PERFORMANCE_DFP_PVID dfp --inner join hliang.PERFORMANCE_OMNI_PVID omni
    INNER JOIN hliang.PERFORMANCE_OMNI_PVID omni ON dfp.PVID = omni.PVID
    AND dfp.page_view_dt = omni.page_view_dt
    LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON omni.SITE_NM = ref.SITE_NM
    AND omni.traffic_source = ref.TRAFFIC_SOURCE_NM
    LEFT JOIN azahn.vw_TopicID topic ON omni.SUBJECT_CD = topic.TOPIC_ID
WHERE
    dfp.page_View_Dt BETWEEN :VStartTime
    AND :VEndTime --and omni.page_View_Dt between :VStartTime and :VEndTime
;

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    HOLDING_IMPS_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PERFORMANCE_HOLDING_IMPS (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dtm) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_HOLDING_all h
        WHERE
            h.media_type = 'Media'
        GROUP BY
            trunc(page_view_dtm),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_HOLDING_all');

/************************************************************************************************************
 
 Summarize all DFP PVID (regardless of SFID to join to Sponsor Pages
 
 ************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    EXCEPTIONS_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PERFORMANCE_OMNI_EXCEPTIONS (New)' AS Tablename,
            'ALL' AS PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            dual
    );

COMMIT;

------------------------------------------------------ TRUNCATE ------------------------------------------------------
\ qecho TRUNCATE EXCEPTIONS_LOG TRUNCATE TABLE hliang.PERFORMANCE_OMNI_EXCEPTIONS;

-------------------------------------------------------- INSERT ------------------------------------------------------
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_OMNI_EXCEPTIONS
SELECT
    imp.pvid,
    trunc(imp.eventtime) AS page_view_dt,
    sum(
        CASE
            WHEN ad.company_name NOT ilike '%webmd%'
            AND ad.company_name NOT ilike '%house%'
            AND productfamily <> 'Research'
            AND productfamily <> 'TEST'
            AND productfamily <> 'HOUSE' THEN 1
            ELSE 0
        END
    ) AS IMPRESSIONS,
    sum(
        CASE
            WHEN ad.company_name NOT ilike '%webmd%'
            AND ad.company_name NOT ilike '%house%'
            AND productfamily <> 'Research'
            AND productfamily <> 'TEST'
            AND productfamily <> 'HOUSE'
            AND (
                display = 1
                OR imp.pos IN (1923, 923)
            )
            AND imp.ActiveViewEligiblempression = 'Y' THEN 1
            ELSE 0
        END
    ) AS ELIGIBLE,
    sum(
        CASE
            WHEN ad.company_name NOT ilike '%webmd%'
            AND ad.company_name NOT ilike '%house%'
            AND productfamily <> 'Research'
            AND productfamily <> 'TEST'
            AND productfamily <> 'HOUSE'
            AND imp.ActiveViewEligiblempression = 'Y'
            AND nvl(imp.measurableimpression, 'Y') = 'Y' THEN 1
            ELSE 0
        END
    ) AS MEASURABLE,
    sum(
        CASE
            WHEN ad.company_name NOT ilike '%webmd%'
            AND ad.company_name NOT ilike '%house%'
            AND productfamily <> 'Research'
            AND productfamily <> 'TEST'
            AND productfamily <> 'HOUSE'
            AND imp.viewableimpression = 'Y' THEN 1
            ELSE 0
        END
    ) AS VIEWABLE,
    sum(
        CASE
            WHEN company_name NOT ilike '%webmd%'
            AND company_name NOT ilike '%house%'
            AND productfamily <> 'Research'
            AND productfamily <> 'TEST'
            AND productfamily <> 'HOUSE' THEN click_count
            ELSE 0
        END
    ) AS CLICKS
FROM
    dfp.impression imp
    INNER JOIN lfelix.dfp_admanager ad ON imp.lineitemid = ad.lineitem_id
    AND imp.creativeid = ad.creative_id
    INNER JOIN dfp.lkup_position lpos ON imp.pos = lpos.pos -- display ads only, added 1/9/2020
WHERE
    imp.eventtime BETWEEN :VStartTime
    AND :VEndTime --and (ad.CREATIVE_ADDSIZE not ilike '1x%' or (ad.lineitem_id in (4931275628, 4931275151, 4931275142, 4931275622, 4931275145, 4931275133, 4931275148, 4931275631, 4931275154, 4931275130, 4931275610, 4931275625, 4931275613) and ad.lineitem_name ilike '%poster unit%' and ad.company_name = 'J&J - Aveeno Baby - Consumer'))  -- 3/29/19, Kwang Han - special use case for 'Poster Unit' line items in Aveeno Baby campaign
    --and ad.CREATIVE_ADDSIZE not ilike '1x%'
    --and ad.CREATIVE_ADDSIZE not ilike '2x%'
    AND (
        display = 1
        OR imp.pos IN (923, 5000)
    )
    AND productfamily <> 'TEST'
    AND productfamily <> 'Sponsorship Drivers'
    AND IFNULL(creativetemplate_id, 0) NOT IN (10026434, 10113914, 11847431, 12017266, 10023314)
GROUP BY
    trunc(imp.eventtime),
    imp.pvid;

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    EXCEPTIONS_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PERFORMANCE_OMNI_EXCEPTIONS (New)' AS name,
            'ALL' AS PROGRAM_NAME,
            page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_OMNI_EXCEPTIONS
        WHERE
            trunc(page_view_dt) = :VStartTime :: timestamp ---to remove
        GROUP BY
            page_view_dt
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_OMNI_EXCEPTIONS');

/************************************************************************************************************
 
 insert sponsor page detail into holding table
 
 ************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    HOLDING_PV_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PERFORMANCE_HOLDING_PV (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

-------------------------------------------------------- INSERT ------------------------------------------------------
\ qecho
INSERT
    HOLDING_PV
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_HOLDING_all
SELECT
    om.pvid,
    om.VISITOR_ID,
    om.VISIT_NUM,
    om.PAGE_VIEW_NUM,
    om.PAGE_VIEW_DTM,
    om.COUNTRY,
    om.BROWSER_ID,
    om.OS_ID,
    om.SUBJECT_CD,
    nvl(topic.TOPIC_NM, 'UNKNOWN') AS TOPIC_NM,
    om.PAGE_NM,
    om.MOBILE_OPTIMIZED_CD,
    om.DEVICE_TYPE_NM,
    om.SITE_NM,
    ref.TRAFFIC_SOURCE_CLASS,
    ref.TRAFFIC_SOURCE_GROUP,
    --case when om.traffic_source = 'referring module' and om.page_view_num = 1 then 'seo' else ref.traffic_source_class end as traffic_source_class, ---to account for first pv referring module
    --case when om.traffic_source = 'referring module' and om.page_view_num = 1 then 'direct' else ref.traffic_source_group end as traffic_source_group,---to account for first pv referring module
    om.TRAFFIC_SOURCE,
    om.SPONSOR_CLIENT_NM,
    om.SPONSOR_BRAND_NM,
    om.SPONSOR_PROGRAM_NM,
    NULL AS ADVERTISER,
    mg.PROGRAM_NAME,
    NULL :: integer AS LINEITEM_ID,
    NULL :: integer AS ORDERS_ID,
    NULL AS ORDERS_NAME,
    NULL AS PRODUCT_NAME,
    NULL AS CREATIVE_NAME,
    NULL AS ADSIZE,
    NULL :: integer AS CREATIVE_ID,
    NULL AS SFNUMBER,
    split_part(om.Sponsor_program_nm, '_', 3) AS Media_Group,
    'Sponsor_Page' AS Media_Type,
    1 AS Type_Priority,
    (
        CASE
            WHEN om.SPONSOR_PROGRAM_NM ilike '%decision%select%'
            OR om.sponsor_program_nm ilike '%decselect%' THEN 1
            ELSE 2
        END
    ) AS Group_Priority,
    om.page_Views,
    nvl(t.ELIGIBLE, 0) AS ELIGIBLE_IMPS,
    nvl(t.MEASURABLE, 0) AS MEASURABLE_IMPS,
    nvl(t.VIEWABLE, 0) AS VIEWABLE_IMPS,
    nvl(t.clicks, 0) AS CLICKS,
    (
        CASE
            WHEN t.pvid IS NULL THEN 0
            WHEN t.IMPRESSIONS = 0 THEN 1
            ELSE 0
        END
    ) AS EXCLUDE_IMP,
    split_part(om.Sponsor_program_nm, '_', 3) AS AUDIENCE_TARGET --added 9/16/16
,
    NULL AS lf_media_group,
    nvl(t.impressions, 0) AS impressions
FROM
    hliang.PERFORMANCE_OMNI_PVID om
    LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON om.site_nm = ref.site_nm
    AND om.traffic_source = ref.traffic_source_nm
    LEFT JOIN azahn.vw_TopicID topic ON om.SUBJECT_CD = topic.topic_id
    LEFT JOIN hliang.PERFORMANCE_OMNI_EXCEPTIONS t ON om.pvid = t.pvid
    AND om.page_View_dt = t.page_View_dt
    INNER JOIN (
        SELECT
            DISTINCT sf.PROGRAM_NAME
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    ) mg --  on om.sponsor_program_nm ilike '%'||mg.program_name||'%'  --updated from brand to program 7/22 AZ
    ON om.sponsor_program_nm ilike '%' ||CASE
        WHEN mg.program_name = 'XARELTO COMBINED' THEN 'XARELTO'
        ELSE mg.program_name
    END || '%' --updated 1/16/20 to accomodate Xarelto
WHERE
    om.page_View_dtm BETWEEN :VStartTime
    AND :VEndTime;

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    HOLDING_PV_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PERFORMANCE_HOLDING_PV (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dtm) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_HOLDING_all h
        WHERE
            h.media_type <> 'Media'
        GROUP BY
            trunc(page_view_dtm),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_HOLDING_all');

/************************************************************************************************************************
 
 Performance Metrics
 
 *************************************************************************************************************************/
/************************************************************************************************************************
 
 DUU_ByMediaGroup
 
 *************************************************************************************************************************/
-------------------------------------------------------- LOG ------------------------------------------------------
\ qecho
INSERT
    DUU_MG_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'DUU_BYMEDIAGROUP (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

-------------------------------------------------------- INSERT ------------------------------------------------------
\ qecho
INSERT
    DUU_MG_METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        ADVERTISER,
        PAGE_VIEW_DT,
        COUNTRY,
        PLATFORM,
        DEVICE_TYPE_NM,
        MEDIA_GROUP,
        MEDIA_TYPE,
        PROGRAM_VISITORS,
        PROGRAM_MEASURABLE_VISITORS,
        PROGRAM_VIEWABLE_VISITORS,
        IMPRESSIONS,
        MEASURABLE_IMPRESSIONS,
        VIEWABLE_IMPRESSIONS,
        AUDIENCE_TARGET,
        IMPRESSIONS_only
    ) (
        SELECT
            'DUU_BYMEDIAGROUP' AS REPORT_METRIC,
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.page_View_dtm) AS page_view_dt,
            h.COUNTRY,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ) AS Platform,
            nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
            h.MEDIA_GROUP,
            h.MEDIA_TYPE,
            count(DISTINCT h.visitor_ID) AS PROGRAM_VISITORS,
            count(
                DISTINCT CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.visitor_Id
                    WHEN h.media_type = 'Media'
                    AND h.MEASURABLE_IMPS > 0 THEN h.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_MEASURABLE_VISITORS,
            count(
                DISTINCT CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.visitor_Id
                    WHEN h.media_type = 'Media'
                    AND h.VIEWABLE_IMPS > 0 THEN h.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_VIEWABLE_VISITORS,
            sum(h.IMPRESSIONS) AS IMPRESSIONS,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                    ELSE h.MEASURABLE_IMPS
                END
            ) AS MEASURABLE_IMPRESSIONS,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                    ELSE h.VIEWABLE_IMPS
                END
            ) AS VIEWABLE_IMPRESSIONS,
            h.AUDIENCE_TARGET,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.display
                    ELSE h.impressions
                END
            ) AS IMPRESSIONS_only
        FROM
            hliang.PERFORMANCE_holding_all h
            LEFT JOIN hliang.PERFORMANCE_METRICS_all h2 ON h.PROGRAM_NAME = h2.PROGRAM_NAME
            AND trunc(h.page_View_dtm) = h2.PAGE_VIEW_DT
            AND h2.REPORT_METRIC = 'DUU_BYMEDIAGROUP'
            AND h2.page_View_dt BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
        WHERE
            (
                --(h.media_type = 'Media' and h.media_group in('SP','FIXED') and upper(nvl(h.sponsor_program_nm,'ntc')) not ilike '%'||h.PROGRAM_NAME||'%')
                --or (h.media_type = 'Media' and nvl(h.media_group,'OTHER') not in('SP','FIXED'))
                (
                    h.media_type = 'Media'
                    AND h.media_group ~~* '%sponsorship%'
                    AND upper(nvl(h.sponsor_program_nm, 'ntc')) NOT ilike '%' || h.PROGRAM_NAME || '%'
                ) -- updated 2/12/2020 to accomodate adbook swap
                OR (
                    h.media_type = 'Media'
                    AND nvl(h.media_group, 'OTHER') !~~* '%sponsorship%'
                ) -- updated 2/12/2020 to accomodate adbook swap
                OR h.media_type = 'Sponsor_Page'
            )
            AND h.page_View_dtm BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
            AND h2.REPORT_METRIC IS NULL --(h.media_type = 'Media' and h.media_group = 'FIXED' and h.PROGRAM_NAME <> upper(nvl(h.sponsor_program_nm,'ntc')))
        GROUP BY
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.page_View_dtm),
            h.COUNTRY,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ),
            nvl(h.DEVICE_TYPE_NM, 'PC'),
            h.MEDIA_GROUP,
            h.MEDIA_TYPE,
            h.AUDIENCE_TARGET
    );

COMMIT;

-------------------------------------------------------- LOG ------------------------------------------------------
\ qecho
UPDATE
    DUU_MG_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'DUU_BYMEDIAGROUP (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'DUU_BYMEDIAGROUP'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************************
 
 DUU_BYMEDIAGROUP_TOPIC
 
 ************************************************************************************************************************/
-------------------------------------------------------- LOG ------------------------------------------------------
\ qecho
INSERT
    DUU_MG_TOPIC_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'DUU_BYMEDIAGROUP_TOPIC (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

-------------------------------------------------------- INSERT ------------------------------------------------------
\ qecho
INSERT
    DUU_MG_TOPIC_METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all (
        REPORT_METRIC,
        PROGRAM_NAME,
        ADVERTISER,
        PAGE_VIEW_DT,
        MEDIA_TYPE,
        MEDIA_GROUP,
        SUBJECT_CD,
        TOPIC_NM,
        COUNTRY,
        PLATFORM,
        DEVICE_TYPE_NM,
        PROGRAM_VISITORS,
        PROGRAM_MEASURABLE_VISITORS,
        PROGRAM_VIEWABLE_VISITORS,
        IMPRESSIONS,
        MEASURABLE_IMPRESSIONS,
        VIEWABLE_IMPRESSIONS,
        AUDIENCE_TARGET,
        IMPRESSIONS_only
    ) (
        SELECT
            'DUU_BYMEDIAGROUP_TOPIC' AS REPORT_METRIC,
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.page_View_dtm) AS page_view_dt,
            h.MEDIA_TYPE,
            h.MEDIA_GROUP,
            h.SUBJECT_CD,
            h.TOPIC_NM,
            h.COUNTRY,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ) AS Platform,
            nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
            count(DISTINCT h.visitor_ID) AS PROGRAM_VISITORS,
            count(
                DISTINCT CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.visitor_Id
                    WHEN h.media_type = 'Media'
                    AND h.MEASURABLE_IMPS > 0 THEN h.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_MEASURABLE_VISITORS,
            count(
                DISTINCT CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.visitor_Id
                    WHEN h.media_type = 'Media'
                    AND h.viewable_imps > 0 THEN h.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_VIEWABLE_VISITORS,
            sum(h.IMPRESSIONS) AS IMPRESSIONS,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                    ELSE h.MEASURABLE_IMPS
                END
            ) AS MEASURABLE_IMPRESSIONS,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                    ELSE h.VIEWABLE_IMPS
                END
            ) AS VIEWABLE_IMPRESSIONS,
            h.AUDIENCE_TARGET,
            sum(
                CASE
                    WHEN h.media_type = 'Sponsor_Page' THEN h.display
                    ELSE h.impressions
                END
            ) AS IMPRESSIONS_only
        FROM
            hliang.PERFORMANCE_holding_all h
            LEFT JOIN hliang.PERFORMANCE_METRICS_all h2 ON h.PROGRAM_NAME = h2.PROGRAM_NAME
            AND trunc(h.page_View_dtm) = h2.PAGE_VIEW_DT
            AND h2.REPORT_METRIC = 'DUU_BYMEDIAGROUP_TOPIC'
            AND h2.page_View_dt BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
        WHERE
            (
                --(h.media_type = 'Media' and h.media_group in('SP','FIXED') and upper(nvl(h.sponsor_program_nm,'ntc')) not ilike '%'||h.PROGRAM_NAME||'%')
                --or (h.media_type = 'Media' and nvl(h.media_group,'OTHER') not in('SP','FIXED'))
                (
                    h.media_type = 'Media'
                    AND h.media_group ~~* '%sponsorship%'
                    AND upper(nvl(h.sponsor_program_nm, 'ntc')) NOT ilike '%' || h.PROGRAM_NAME || '%'
                ) -- updated 2/12/2020 to accomodate adbook swap
                OR (
                    h.media_type = 'Media'
                    AND nvl(h.media_group, 'OTHER') !~~* '%sponsorship%'
                ) -- updated 2/12/2020 to accomodate adbook swap
                OR h.media_type = 'Sponsor_Page'
            )
            AND h.PAGE_VIEW_DTM BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
            AND h2.REPORT_METRIC IS NULL
        GROUP BY
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.page_View_dtm),
            h.MEDIA_TYPE,
            h.MEDIA_GROUP,
            h.SUBJECT_CD,
            h.TOPIC_NM,
            h.COUNTRY,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ),
            nvl(h.DEVICE_TYPE_NM, 'PC'),
            h.AUDIENCE_TARGET
    );

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    DUU_MG_TOPIC_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'DUU_BYMEDIAGROUP_TOPIC (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'DUU_BYMEDIAGROUP_TOPIC'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************************
 
 DUU_%_Topic
 
 ************************************************************************************************************************/
------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
INSERT
    SOV_TOPIC_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'SOV_TOPIC (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

/***********TEMP TABLE FOR OMNITURE DATA BY TOPIC BY DAY DEVICE****************************************/
\ qecho TRUNCATE temp omniture data TRUNCATE TABLE hliang.HYL_PERFORMANCE_SOV_TEMP;

\ qecho
INSERT
    temp omniture data
INSERT
    /*+ DIRECT */
    INTO hliang.HYL_PERFORMANCE_SOV_TEMP
SELECT
    trunc(pv.page_View_dtm) AS page_view_dt,
    pv.subject_cd,
    nvl(pv.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
    (
        CASE
            WHEN pv.country_nm = 'usa' THEN 'US'
            ELSE 'xUS'
        END
    ) AS Country,
    count(DISTINCT pv.visitor_ID) AS Visitors
FROM
    omniture_new.core_page_view pv
WHERE
    pv.page_View_dtm BETWEEN :VStartTime
    AND :VEndTime
    AND pv.site_nm IN(
        'core',
        'rxlist',
        'emedicinehealth',
        'medicinenet',
        'onhealth'
    )
GROUP BY
    trunc(pv.page_View_dtm),
    pv.subject_cd,
    nvl(pv.DEVICE_TYPE_NM, 'PC'),
    (
        CASE
            WHEN pv.country_nm = 'usa' THEN 'US'
            ELSE 'xUS'
        END
    );

COMMIT;

SELECT
    analyze_statistics('hliang.HYL_PERFORMANCE_SOV_TEMP');

/***********TEMP TABLE FOR PERFORMANCE DATA BY TOPIC BY DAY DEVICE****************************************/
\ qecho TRUNCATE temp SOV PROGRAM DATA TRUNCATE TABLE hliang.HYL_PERFORMANCE_SOV_TEMP2;

\ qecho
INSERT
INSERT
    TEMP SOV PROGRAM DATA
INSERT
    /*+ DIRECT */
    INTO hliang.HYL_PERFORMANCE_SOV_TEMP2
SELECT
    trunc(h.page_View_dtm) AS PAGE_VIEW_DT,
    h.SUBJECT_CD,
    nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
    h.COUNTRY,
    count(DISTINCT h.visitor_ID) AS VISITORS,
    h.PROGRAM_NAME
FROM
    hliang.PERFORMANCE_holding_all h
WHERE
    (
        --(h.media_type = 'Media' and h.media_group in('SP','FIXED') and upper(nvl(h.sponsor_program_nm,'ntc')) not ilike '%'||h.PROGRAM_NAME||'%')
        --or (h.media_type = 'Media' and nvl(h.media_group,'OTHER') not in('SP','FIXED'))
        (
            h.media_type = 'Media'
            AND h.media_group ~~* '%sponsorship%'
            AND upper(nvl(h.sponsor_program_nm, 'ntc')) NOT ilike '%' || h.PROGRAM_NAME || '%'
        ) -- updated 2/12/2020 to accomodate adbook swap
        OR (
            h.media_type = 'Media'
            AND nvl(h.media_group, 'OTHER') !~~* '%sponsorship%'
        ) -- updated 2/12/2020 to accomodate adbook swap
        OR h.media_type = 'Sponsor_Page'
    )
    AND h.page_View_dtm BETWEEN :VStartTime
    AND :VEndTime
GROUP BY
    trunc(h.page_View_dtm),
    h.SUBJECT_CD,
    nvl(h.DEVICE_TYPE_NM, 'PC'),
    h.COUNTRY,
    h.PROGRAM_NAME;

COMMIT;

SELECT
    analyze_statistics('hliang.HYL_PERFORMANCE_SOV_TEMP2');

/***********JOIN TEMP AND HOLDING FOR SOV insert results****************************************/
\ qecho
INSERT
    SOV_TOPIC_METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all (
        REPORT_METRIC,
        PROGRAM_NAME,
        PAGE_VIEW_DT,
        SUBJECT_CD,
        TOPIC_NM,
        COUNTRY,
        DEVICE_TYPE_NM,
        PROGRAM_VISITORS,
        SITE_VISITORS
    ) (
        SELECT
            DISTINCT 'SOV_TOPIC' AS REPORT_METRIC,
            p.PROGRAM_NAME,
            p.PAGE_VIEW_DT,
            P.SUBJECT_CD,
            topic.TOPIC_NM,
            p.COUNTRY,
            p.DEVICE_TYPE_NM,
            p.VISITORS AS PROGRAM_Visitors,
            o.VISITORS AS TOPIC_VISITORS
        FROM
            hliang.HYL_PERFORMANCE_SOV_TEMP2 p
            LEFT JOIN hliang.HYL_PERFORMANCE_SOV_TEMP o ON p.PAGE_VIEW_DT = o.PAGE_VIEW_DT
            AND p.DEVICE_TYPE_NM = o.DEVICE_TYPE_NM
            AND nvl(p.DEVICE_TYPE_NM, 'PC') = nvl(o.DEVICE_TYPE_NM, 'PC')
            AND p.COUNTRY = o.COUNTRY
            AND p.SUBJECT_CD = o.SUBJECT_CD
            LEFT JOIN azahn.vw_TopicID topic ON p.subject_cd = topic.topic_id
            LEFT JOIN hliang.PERFORMANCE_METRICS_all h2 ON p.PROGRAM_NAME = h2.PROGRAM_NAME
            AND p.page_View_dt = h2.PAGE_VIEW_DT
            AND h2.REPORT_METRIC = 'SOV_TOPIC'
            AND h2.page_View_dt BETWEEN :VStartTime
            AND :VEndTime
        WHERE
            h2.REPORT_METRIC IS NULL
    );

COMMIT;

------------------------------------------------------ LOG ------------------------------------------------------
\ qecho
UPDATE
    SOV_TOPIC_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'SOV_TOPIC (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'SOV_TOPIC'
            AND m.page_View_dt = :VStartTime
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('ahliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************
 
 DFP vs Tracker
 
 ************************************************************************************************************/
-------------------------------------------------------- LOG ------------------------------------------------------
\ qecho
INSERT
    DFP_VS_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'DFP_VS_TRACKER (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

-------------------------------------------------------- INSERT ------------------------------------------------------
\ qecho
INSERT
    DFP_VS_METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        ADVERTISER,
        PAGE_VIEW_DT,
        COUNTRY,
        LINEITEM_ID,
        CREATIVE_ID,
        MEDIA_GROUP,
        PROGRAM_VISITORS,
        SITE_VISITORS
    ) (
        SELECT
            'DFP_VS_TRACKER' AS REPORT_METRIC,
            ad.PROGRAM_NAME,
            ad.ADVERTISER,
            ad.PAGE_VIEW_DT,
            ad.COUNTRY,
            ad.LINEITEM_ID,
            ad.CREATIVE_ID,
            ad.MEDIA_GROUP,
            h.IMPRESSIONS AS TRACKER_IMPRESSIONS,
            ad.IMPRESSIONS AS DFP_IMPRESSIONS --h.IMPRESSIONS - ad.IMPRESSIONS as DIFFERENCE,
        FROM
(
                SELECT
                    ad.PROGRAM_NAME,
                    trunc(ad.eventtime) AS PAGE_VIEW_DT,
                    /***NOTE to replace back trunc(ad.page_view_dtm) as PAGE_VIEW_DT, ***/
                    ad.company_name AS ADVERTISER,
                    /***NOTE to replace back ad.ADVERTISER, ***/
                    ad.LINEITEM_ID,
                    ad.CREATIVE_ID,
                    ad.COUNTRY,
                    ad.MEDIA_GROUP,
                    sum(ad.IMPRESSIONS) AS IMPRESSIONS
                FROM
                    hliang.PERFORMANCE_DFP_PVID ad
                GROUP BY
                    ad.PROGRAM_NAME,
                    trunc(ad.eventtime),
                    /*****NOTE:to replace back trunc(ad.page_view_dtm),****/
                    ad.company_name,
                    /***NOTE to replace back ad.ADVERTISER, ***/
                    ad.LINEITEM_ID,
                    ad.CREATIVE_ID,
                    ad.COUNTRY,
                    ad.MEDIA_GROUP
            ) ad
            LEFT JOIN(
                SELECT
                    h.PROGRAM_NAME,
                    trunc(h.page_view_dtm) AS PAGE_VIEW_DT,
                    h.ADVERTISER,
                    h.LINEITEM_ID,
                    h.CREATIVE_ID,
                    h.MEDIA_GROUP,
                    h.COUNTRY,
                    sum(h.IMPRESSIONS) AS IMPRESSIONS
                FROM
                    hliang.PERFORMANCE_holding_all h
                WHERE
                    h.page_View_dtm BETWEEN :VStartTime
                    AND :VEndTime
                GROUP BY
                    h.PROGRAM_NAME,
                    trunc(h.page_view_dtm),
                    h.ADVERTISER,
                    h.LINEITEM_ID,
                    h.CREATIVE_ID,
                    h.MEDIA_GROUP,
                    h.COUNTRY
            ) h ON ad.LINEITEM_ID = h.LINEITEM_ID
            AND ad.CREATIVE_ID = h.CREATIVE_ID
            AND ad.PAGE_VIEW_DT = h.PAGE_VIEW_DT
            AND AD.COUNTRY = H.COUNTRY
            AND AD.MEDIA_GROUP = h.MEDIA_GROUP
            AND ad.ADVERTISER = h.ADVERTISER
            LEFT JOIN hliang.PERFORMANCE_METRICS_all h2 ON h.PROGRAM_NAME = h2.PROGRAM_NAME
            AND ad.PAGE_VIEW_DT = h2.PAGE_VIEW_DT
            AND h2.REPORT_METRIC = 'DFP_VS_TRACKER'
            AND h2.page_View_dt BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
        WHERE
            h2.report_metric IS NULL
        ORDER BY
            ad.PROGRAM_NAME,
            ad.PAGE_VIEW_DT,
            ad.LINEITEM_ID,
            ad.CREATIVE_ID
    );

COMMIT;

------------------------------------------------------LOG------------------------------------------------------
\ qecho
UPDATE
    DFP_VS_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'DFP_VS_TRACKER (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'DFP_VS_TRACKER'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************
 
 PRIORITY METRICS
 
 ************************************************************************************************************/
------------------------------------------------------LOG------------------------------------------------------
\ qecho
INSERT
    PRIORITY_METRIC_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'PRIORITY_METRIC (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

--------------------------------------------------------DROP PARTITION------------------------------------------------------
----added direct hint 1/30/18
DELETE
/*+ direct */
FROM
    hliang.PERFORMANCE_METRICS_all
WHERE
    report_metric = 'PRIORITY' --and trunc(page_View_dt,'MM') = trunc(:VStartTime::date,'MM')
    AND trunc(page_View_dt, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
    AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
;

COMMIT;

--------------------------------------------------------INSERT------------------------------------------------------
\ qecho
INSERT
    PRIORITY_METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        ADVERTISER,
        PAGE_VIEW_DT,
        COUNTRY,
        SUBJECT_CD,
        TOPIC_NM,
        MEDIA_GROUP,
        MEDIA_TYPE,
        TRAFFIC_SOURCE,
        PLATFORM,
        DEVICE_TYPE_NM,
        PROGRAM_VISITORS,
        PROGRAM_MEASURABLE_VISITORS,
        PROGRAM_VIEWABLE_VISITORS,
        AUDIENCE_TARGET
    ) (
        SELECT
            'PRIORITY' AS Report_Metric,
            p.PROGRAM_NAME,
            p.ADVERTISER,
            trunc(p.page_View_dtm) AS Priority_month,
            p.COUNTRY,
            p.SUBJECT_CD,
            p.TOPIC_NM,
            p.MEDIA_GROUP,
            p.MEDIA_TYPE,
            (
                CASE
                    WHEN TRAFFIC_SOURCE_GROUP IN('content marketing', 'internal traffic') THEN TRAFFIC_SOURCE_GROUP
                    ELSE TRAFFIC_SOURCE_CLASS
                END
            ) AS TRAFFIC_SOURCE,
            (
                CASE
                    WHEN p.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN p.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ) AS PLATFORM,
            nvl(p.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
            count(DISTINCT p.visitor_ID) AS PROGRAM_VISITORS,
            count(
                DISTINCT CASE
                    WHEN v.MEASURABLE_IMPRESSIONS > 0 THEN p.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_MEASURABLE_VISITORS,
            count(
                DISTINCT CASE
                    WHEN v.VIEWABLE_IMPRESSIONS > 0 THEN p.visitor_ID
                    ELSE NULL
                END
            ) AS PROGRAM_VIEWABLE_VISITORS,
            p.AUDIENCE_TARGET
        FROM
            (
                SELECT
                    *,
                    row_number() over (
                        PARTITION by h.visitor_Id,
                        h.program_name,
                        trunc(h.page_view_dtm, 'MM')
                        ORDER BY
                            h.visitor_id,
                            h.TYPE_PRIORITY,
                            h.GROUP_PRIORITY,
                            h.visit_num,
                            h.PAGE_VIEW_NUM,
                            h.page_View_dtm
                    ) AS Priority_Row
                FROM
                    hliang.PERFORMANCE_holding_all h
                WHERE
                    --        trunc(h.page_View_dtm,'MM') = trunc(:VStartTime::date,'MM') 
                    trunc(h.page_View_dtm, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
                    AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
                    AND (
                        --		--(h.media_type = 'Media' and h.media_group in('SP','FIXED') and upper(nvl(h.sponsor_program_nm,'ntc')) not ilike '%'||h.PROGRAM_NAME||'%')
                        --		--or (h.media_type = 'Media' and nvl(h.media_group,'OTHER') not in('SP','FIXED'))
                        (
                            h.media_type = 'Media'
                            AND h.media_group ~~* '%sponsorship%'
                            AND upper(nvl(h.sponsor_program_nm, 'ntc')) NOT ilike '%' || h.PROGRAM_NAME || '%'
                        ) -- updated 2/12/2020 to accomodate adbook swap
                        OR (
                            h.media_type = 'Media'
                            AND nvl(h.media_group, 'OTHER') !~~* '%sponsorship%'
                        ) -- updated 2/12/2020 to accomodate adbook swap
                        OR h.media_type = 'Sponsor_Page'
                    )
                    /***********to remove later**************/
                    --        and h.page_View_dtm between :VStartTime and :VEndTime
            ) p
            INNER JOIN (
                SELECT
                    h.visitor_Id,
                    h.program_name,
                    trunc(h.page_View_dtm, 'MM') AS MONTH,
                    sum(
                        CASE
                            WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                            ELSE h.MEASURABLE_IMPS
                        END
                    ) AS MEASURABLE_IMPRESSIONS,
                    sum(
                        CASE
                            WHEN h.media_type = 'Sponsor_Page' THEN h.impressions
                            ELSE h.VIEWABLE_IMPS
                        END
                    ) AS VIEWABLE_IMPRESSIONS
                FROM
                    hliang.PERFORMANCE_holding_all h --        --where h.page_View_dtm between '2016-02-01 00:00:00' and '2016-02-01 23:59:59'
                    --      where trunc(h.page_View_dtm,'MM') = trunc(:Find_Date_loop.startTime,'MM')
                WHERE
                    trunc(h.page_View_dtm, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
                    AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
                    /***********to remove later**************/
                    --        and h.page_View_dtm between :VStartTime and :VEndTime
                GROUP BY
                    h.visitor_Id,
                    h.program_name,
                    trunc(h.page_View_dtm, 'MM')
            ) v ON p.visitor_Id = v.visitor_id
            AND p.PROGRAM_NAME = v.PROGRAM_NAME
            AND trunc(p.page_view_dtm, 'MM') = v.month
            LEFT JOIN hliang.PERFORMANCE_METRICS_all m ON trunc(p.page_view_dtm, 'MM') = trunc(m.page_View_dt, 'MM') --trunc(p.page_View_dtm,'MM') between trunc(:VStartTime::date,'MM') and trunc(:VEndTime::date,'MM')  ----to replace soon for daily
            AND p.program_name = m.program_name
            AND m.REPORT_METRIC = 'PRIORITY' --and m.page_view_dt = trunc(:VStartTime::date,'MM')
            AND m.page_view_dt BETWEEN trunc(:VStartTime :: date, 'MM')
            AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
        WHERE
            p.Priority_Row = 1
            AND m.PROGRAM_NAME IS NULL
        GROUP BY
            p.PROGRAM_NAME,
            p.ADVERTISER,
            trunc(p.page_View_dtm),
            p.COUNTRY,
            p.SUBJECT_CD,
            p.TOPIC_NM,
            p.MEDIA_GROUP,
            p.MEDIA_TYPE,
            (
                CASE
                    WHEN TRAFFIC_SOURCE_GROUP IN('content marketing', 'internal traffic') THEN TRAFFIC_SOURCE_GROUP
                    ELSE TRAFFIC_SOURCE_CLASS
                END
            ),
            (
                CASE
                    WHEN p.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN p.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ),
            nvl(p.DEVICE_TYPE_NM, 'PC'),
            p.AUDIENCE_TARGET
    );

COMMIT;

------------------------------------------------------LOG------------------------------------------------------
\ qecho
UPDATE
    PRIORITY_METRIC_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'PRIORITY_METRIC (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'PRIORITY'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************
 
 IMPS per DUU
 
 ************************************************************************************************************/
------------------------------------------------------LOG------------------------------------------------------
\ qecho
INSERT
    IMPS_DUU_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'IMPS_PER_DUU (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

--------------------------------------------------------INSERT------------------------------------------------------
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        PAGE_VIEW_DT,
        MEDIA_GROUP,
        COUNTRY,
        PLATFORM,
        DEVICE_TYPE_NM,
        VARIOUS,
        PROGRAM_VISITORS,
        AUDIENCE_TARGET
    )(
        SELECT
            'IMPS_PER_DUU' AS REPORT_METRIC,
            x.PROGRAM_NAME,
            x.PAGE_VIEW_DT,
            x.MEDIA_GROUP,
            x.COUNTRY,
            x.PLATFORM,
            x.DEVICE_TYPE_NM,
            (
                CASE
                    WHEN x.IMPRESSIONS < 11 THEN x.IMPRESSIONS :: varchar
                    ELSE '11+'
                END
            ) AS IMPRESSIONS,
            count(DISTINCT x.Visitor_ID) AS VISITORS,
            x.AUDIENCE_TARGET
        FROM
(
                SELECT
                    h.PROGRAM_NAME,
                    trunc(h.page_View_dtm) AS PAGE_VIEW_DT,
                    MEDIA_GROUP,
                    h.VISITOR_ID,
                    h.AUDIENCE_TARGET,
                    (
                        CASE
                            WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                            WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                            ELSE 'Desktop'
                        END
                    ) AS Platform,
                    nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
                    h.COUNTRY,
                    sum(impressions) AS IMPRESSIONS
                FROM
                    hliang.PERFORMANCE_holding_all h
                WHERE
                    h.media_type = 'Media'
                    AND h.page_View_dtm BETWEEN :VStartTime
                    AND :VEndTime --added 5/29/18
                GROUP BY
                    h.PROGRAM_NAME,
                    trunc(h.page_View_dtm),
                    h.VISITOR_ID,
                    h.AUDIENCE_TARGET,
                    MEDIA_GROUP,
                    (
                        CASE
                            WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                            WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                            ELSE 'Desktop'
                        END
                    ),
                    nvl(h.DEVICE_TYPE_NM, 'PC'),
                    h.COUNTRY
            ) x
            LEFT JOIN hliang.PERFORMANCE_METRICS_all m ON x.PROGRAM_NAME = m.PROGRAM_NAME
            AND x.PAGE_VIEW_DT = m.PAGE_VIEW_DT
            AND m.REPORT_METRIC = 'IMPS_PER_DUU'
            AND m.page_View_dt BETWEEN :VStartTime
            AND :VEndTime --added 5/29/18
        WHERE
            m.program_name IS NULL
        GROUP BY
            x.PROGRAM_NAME,
            x.PAGE_VIEW_DT,
            x.MEDIA_GROUP,
            x.COUNTRY,
            x.PLATFORM,
            x.DEVICE_TYPE_NM,
            (
                CASE
                    WHEN x.IMPRESSIONS < 11 THEN x.IMPRESSIONS :: varchar
                    ELSE '11+'
                END
            ),
            x.AUDIENCE_TARGET
    );

COMMIT;

--------------------------------------------------------LOG------------------------------------------------------
\ qecho
UPDATE
    IMPS_DUU_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'IMPS_PER_DUU (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'IMPS_PER_DUU'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

/************************************************************************************************************
 
 TS_MUU_FIRSTSEEN METRICS
 
 ************************************************************************************************************/
--------------------------------------------------------LOG------------------------------------------------------
\ qecho
INSERT
    TS_MUU_FIRSTSEEN_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'TS_MUU_FIRSTSEEN (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

--------------------------------------------------------DROP PARTITION------------------------------------------------------
----added direct hint 1/30/18
\ qecho DELETE TS_MUU_FIRSTSEEN METRIC DELETE
/*+ direct */
FROM
    hliang.PERFORMANCE_METRICS_all
WHERE
    report_metric = 'TS_MUU_FIRSTSEEN' --and trunc(page_View_dt,'MM') = trunc(:VStartTime::date,'MM')
    AND trunc(page_View_dt, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
    AND trunc(:VEndTime :: date, 'MM');

--------------------------------------------------------INSERT------------------------------------------------------
\ qecho
INSERT
    TS_MUU_FIRSTSEEN METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        PAGE_VIEW_DT,
        COUNTRY,
        TRAFFIC_SOURCE,
        PLATFORM,
        DEVICE_TYPE_NM,
        MEDIA_TYPE,
        MEDIA_GROUP,
        PROGRAM_VISITORS,
        AUDIENCE_TARGET,
        IMPRESSIONS,
        CLICKS,
        IMPRESSIONS_only
    ) (
        SELECT
            'TS_MUU_FIRSTSEEN' AS REPORT_METRIC,
            h.PROGRAM_NAME,
            trunc(h.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
            h.COUNTRY,
            (
                CASE
                    WHEN TRAFFIC_SOURCE_GROUP IN('content marketing', 'internal traffic') THEN TRAFFIC_SOURCE_GROUP
                    ELSE TRAFFIC_SOURCE_CLASS
                END
            ) AS TRAFFIC_SOURCE,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ) AS PLATFORM,
            nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
            h.MEDIA_TYPE,
            h.MEDIA_GROUP,
            count(DISTINCT h.VISITOR_ID) AS VISITORS,
            h.AUDIENCE_TARGET,
            sum(h.IMPS) AS IMPRESSIONS,
            sum(h.CLKS) AS CLICKS,
            sum(h.imps_only) AS impressions_only -----to remove later
        FROM
(
                SELECT
                    h.*,
                    row_number() over (
                        PARTITION by PROGRAM_NAME,
                        trunc(PAGE_VIEW_DTM, 'MM'),
                        VISITOR_ID
                        ORDER BY
                            PROGRAM_NAME,
                            VISITOR_ID,
                            PAGE_VIEW_DTM,
                            PAGE_VIEW_NUM
                    ) AS Row_num,
                    sum(h.impressions) over (
                        PARTITION by PROGRAM_NAME,
                        trunc(PAGE_VIEW_DTM, 'MM'),
                        VISITOR_ID
                        ORDER BY
                            PROGRAM_NAME,
                            VISITOR_ID,
                            PAGE_VIEW_DTM DESC,
                            PAGE_VIEW_NUM DESC
                    ) AS IMPS,
                    sum(h.CLICKS) over (
                        PARTITION by PROGRAM_NAME,
                        trunc(PAGE_VIEW_DTM, 'MM'),
                        VISITOR_ID
                        ORDER BY
                            PROGRAM_NAME,
                            VISITOR_ID,
                            PAGE_VIEW_DTM DESC,
                            PAGE_VIEW_NUM DESC
                    ) AS CLKS,
                    sum(
                        CASE
                            WHEN h.media_type = 'Sponsor_Page' THEN h.display
                            ELSE h.impressions
                        END
                    ) over (
                        PARTITION by PROGRAM_NAME,
                        trunc(PAGE_VIEW_DTM, 'MM'),
                        VISITOR_ID
                        ORDER BY
                            PROGRAM_NAME,
                            VISITOR_ID,
                            PAGE_VIEW_DTM DESC,
                            PAGE_VIEW_NUM DESC
                    ) AS IMPS_only
                FROM
                    hliang.PERFORMANCE_holding_all h
                WHERE
                    (
                        --		--(h.media_type = 'Media' and h.media_group in('SP','FIXED') and upper(nvl(h.sponsor_program_nm,'ntc')) not ilike '%'||h.PROGRAM_NAME||'%')
                        --		--or (h.media_type = 'Media' and nvl(h.media_group,'OTHER') not in('SP','FIXED'))
                        (
                            h.media_type = 'Media'
                            AND h.media_group ~~* '%sponsorship%'
                            AND upper(nvl(h.sponsor_program_nm, 'ntc')) NOT ilike '%' || h.PROGRAM_NAME || '%'
                        ) -- updated 2/12/2020 to accomodate adbook swap
                        OR (
                            h.media_type = 'Media'
                            AND nvl(h.media_group, 'OTHER') !~~* '%sponsorship%'
                        ) -- updated 2/12/2020 to accomodate adbook swap
                        OR h.media_type = 'Sponsor_Page'
                    )
                    AND --      trunc(h.page_View_dtm,'MM') = trunc(:VStartTime::date,'MM')
                    /****to remove later****/
                    trunc(h.page_View_dtm, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
                    AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
                    --        and h.page_View_dtm between :VStartTime and :VEndTime
            ) h
            LEFT JOIN hliang.PERFORMANCE_METRICS_all m ON trunc(h.page_view_dtm) = m.page_View_dt
            AND h.program_name = m.program_name
            AND m.REPORT_METRIC = 'TS_MUU_FIRSTSEEN' ----and m.page_View_dt = trunc(:VStartTime::date,'MM') --changed 5/30/18
            --and trunc(m.page_View_dt,'MM') = trunc(:VStartTime::date,'MM')
            AND trunc(m.page_View_dt, 'MM') BETWEEN trunc(:VStartTime :: date, 'MM')
            AND trunc(:VEndTime :: date, 'MM') ----to replace soon for daily
        WHERE
            h.row_num = 1
            AND m.PROGRAM_NAME IS NULL
        GROUP BY
            h.PROGRAM_NAME,
            trunc(h.PAGE_VIEW_DTM),
            h.COUNTRY,
            (
                CASE
                    WHEN TRAFFIC_SOURCE_GROUP IN('content marketing', 'internal traffic') THEN TRAFFIC_SOURCE_GROUP
                    ELSE TRAFFIC_SOURCE_CLASS
                END
            ),
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ),
            nvl(h.DEVICE_TYPE_NM, 'PC'),
            h.MEDIA_TYPE,
            h.MEDIA_GROUP,
            h.AUDIENCE_TARGET
    );

COMMIT;

------------------------------------------------------LOG------------------------------------------------------
\ qecho
UPDATE
    PRIORITY_METRIC_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'TS_MUU_FIRSTSEEN (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'TS_MUU_FIRSTSEEN'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

/************************************************************************************************************
 
 CTR METRICS
 
 ************************************************************************************************************/
--------------------------------------------------------LOG------------------------------------------------------
\ qecho
INSERT
    CTR_LOG
INSERT INTO
    hliang.PERFORMANCE_LOG(
        SELECT
            DISTINCT 'CTR (New)' AS Tablename,
            PROGRAM_NAME,
            :VStartTime :: timestamp AS Load_Date,
            NULL AS records,
            sysdate AS Start_Time,
            NULL AS end_time
        FROM
            azahn.PERFORMANCE_SFNUMBERS sf
    );

COMMIT;

--------------------------------------------------------INSERT------------------------------------------------------
\ qecho
INSERT
    CTR METRIC
INSERT
    /*+ DIRECT */
    INTO hliang.PERFORMANCE_METRICS_all(
        REPORT_METRIC,
        PROGRAM_NAME,
        ADVERTISER,
        PAGE_VIEW_DT,
        COUNTRY,
        MEDIA_GROUP,
        PLATFORM,
        DEVICE_TYPE_NM,
        CLICKS,
        IMPRESSIONS,
        AUDIENCE_TARGET
    )(
        SELECT
            'CTR' AS REPORT_METRIC,
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
            h.COUNTRY,
            h.MEDIA_GROUP,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ) AS PLATFORM,
            nvl(h.DEVICE_TYPE_NM, 'PC') AS DEVICE_TYPE_NM,
            sum(h.CLICKS) AS CLICKS,
            sum(h.IMPRESSIONS) AS IMPRESSIONS,
            h.AUDIENCE_TARGET
        FROM
            hliang.PERFORMANCE_holding_all h
            LEFT JOIN hliang.PERFORMANCE_METRICS_all m ON h.PROGRAM_NAME = m.PROGRAM_NAME
            AND trunc(h.PAGE_VIEW_DTM) = m.PAGE_VIEW_DT
            AND m.REPORT_METRIC = 'CTR' --and m.page_View_dt = '2023-08-24 00:00:00' 
            AND m.page_View_dt BETWEEN :VStartTime
            AND :VEndTime ----to remove later
        WHERE
            m.PROGRAM_NAME IS NULL
            AND h.MEDIA_TYPE = 'Media'
            AND h.page_View_dtm BETWEEN :VStartTime
            AND :VEndTime
        GROUP BY
            h.PROGRAM_NAME,
            h.ADVERTISER,
            trunc(h.PAGE_VIEW_DTM),
            h.COUNTRY,
            h.MEDIA_GROUP,
            (
                CASE
                    WHEN h.MOBILE_OPTIMIZED_CD = 1 THEN 'Mobile'
                    WHEN h.MOBILE_OPTIMIZED_CD = 2 THEN 'Responsive'
                    ELSE 'Desktop'
                END
            ),
            nvl(h.DEVICE_TYPE_NM, 'PC'),
            h.AUDIENCE_TARGET
    );

COMMIT;

------------------------------------------------------LOG------------------------------------------------------
\ qecho
UPDATE
    CTR_LOG
UPDATE
    hliang.PERFORMANCE_LOG
SET
    RECORDS_PROCESSED = n.counts,
    END_TIME = sysdate
FROM
    (
        SELECT
            'CTR (New)' AS name,
            PROGRAM_NAME,
            trunc(page_view_dt) AS page_view_dt,
            count(*) AS counts
        FROM
            hliang.PERFORMANCE_METRICS_all m
        WHERE
            m.REPORT_METRIC = 'CTR'
        GROUP BY
            trunc(page_view_dt),
            PROGRAM_NAME
    ) n
WHERE
    PERFORMANCE_LOG.tablename = n.name
    AND PERFORMANCE_LOG.load_date = n.page_view_dt
    AND PERFORMANCE_LOG.PROGRAM_NAME = n.PROGRAM_NAME
    AND PERFORMANCE_LOG.RECORDS_PROCESSED IS NULL;

COMMIT;

SELECT
    analyze_statistics('hliang.PERFORMANCE_METRICS_all');

\ o Performance_DataGather_Auto.sql Displaying Performance_DataGather_Auto.sql.