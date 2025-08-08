\ o :VLog
/******************************************************************************************************************************
 
 SUMMARIZE DFP at PVID
 using length(requestedadunitsize) >3 to remove all 1x1s etc
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP1 log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_TEMP1' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho TRUNCATE TS7_TEMP1 TRUNCATE TABLE azahn.TS7_TEMP1;

\ qecho
INSERT
        TS7_TEMP1 impressions --create table azahn.TS7_TEMP1 as(
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP1(
                SELECT
                        PVID,
                        trunc(imp.eventtime) AS PAGE_VIEW_DT,
                        /************** TOTAL IMPS **************/
                        count(*) AS TOTAL_IMPRESSIONS,
                        sum(
                                CASE
                                        WHEN imp.ActiveViewEligiblempression = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_ELIGIBLE_IMPS,
                        sum(
                                CASE
                                        WHEN imp.ActiveViewEligiblempression = 'Y'
                                        AND nvl(imp.measurableimpression, 'Y') = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_MEASURABLE_IMPS,
                        sum(
                                CASE
                                        WHEN imp.viewableimpression = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_VIEWABLE_IMPS,
                        sum(imp.CLICK_COUNT) AS TOTAL_CLICKS,
                        /************** FILLED UNFILLED **************/
                        sum(
                                CASE
                                        WHEN imp.lineitemid > 0
                                        AND imp.creativeid > 0 THEN 1
                                        ELSE 0
                                END
                        ) AS FILLED_IMPS,
                        sum(
                                CASE
                                        WHEN imp.lineitemid = 0
                                        AND imp.creativeid = 0 THEN 1
                                        ELSE 0
                                END
                        ) AS UNFILLED_IMPS,
                        /************** HOUSE IMPS **************/
                        sum(
                                CASE
                                        WHEN ad.company_type = 'HOUSE' THEN 1
                                        ELSE 0
                                END
                        ) AS HOUSE_IMPS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'HOUSE' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS HOUSE_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'PROGRAMATIC' THEN 1
                                        ELSE 0
                                END
                        ) AS PROGRAMATIC_IMPS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'PROGRAMATIC' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS PROGRAMATIC_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Content Adjacency' THEN 1
                                        ELSE 0
                                END
                        ) AS CC_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Content Adjacency' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CC_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Audience' THEN 1
                                        ELSE 0
                                END
                        ) AS CDT_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Audience' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CDT_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Cortex' THEN 1
                                        ELSE 0
                                END
                        ) AS CMT_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Cortex' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CMT_CLICKS
                FROM
                        dfp.impression imp
                        LEFT JOIN azahn.vw_dfp_admanager_v3 ad ON imp.creativeid = ad.creative_id
                        AND imp.lineitemid = ad.lineitem_id
                        INNER JOIN (
                                SELECT
                                        DISTINCT POS
                                FROM
                                        dfp.lkup_position
                                WHERE
                                        DISPLAY = 1
                        ) pos --changed from billable_position table 2/5/18
                        ON imp.pos = pos.pos
                WHERE
                        imp.eventtime BETWEEN :VStartTime
                        AND :VEndTime --imp.eventtime between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'
                        AND imp.env = 0 --and length(imp.requestedadunitsizes) > 5
                GROUP BY
                        pvid,
                        trunc(imp.eventtime)
        ) --order by
        --pvid
        --segmented by hash(pvid) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP1');

\ qecho
UPDATE
        TS7_TEMP1 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time
FROM
        (
                SELECT
                        'TS7_TEMP1' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        --        sum(case when site_Nm = 'core' then page_Views else 0 end) as Core_PV,
                        --        suM(case when site_nm in('core','medicinenet','emedicinehealth','medterms','rxlist') then page_Views else 0 end) as ConsNetwork_PV,
                        --        suM(case when site_nm in('medscape','emedicine','cme','mscp','mdedge','medscape','medscape-fr','medscape-de','medscape-es','medscape-pt') then page_Views else 0 end) as ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_TEMP1
                WHERE
                        page_view_dt = :VStartTime
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 SUMMARIZE DFP at PVID and COMPANY
 using length(requestedadunitsize) >3 to remove all 1x1s etc
 added 8/3/18
 
 ******************************************************************************************************************************/
\ qecho TRUNCATE TS7_TEMP1 TRUNCATE TABLE azahn.TS7_TEMP1A;

\ qecho
INSERT INTO
        TS7_TEMP1
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP1A(
                SELECT
                        PVID,
                        trunc(imp.eventtime) AS PAGE_VIEW_DT,
                        ad.COMPANY_NAME,
                        /************** TOTAL IMPS **************/
                        count(*) AS TOTAL_IMPRESSIONS,
                        sum(
                                CASE
                                        WHEN imp.ActiveViewEligiblempression = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_ELIGIBLE_IMPS,
                        sum(
                                CASE
                                        WHEN imp.ActiveViewEligiblempression = 'Y'
                                        AND nvl(imp.measurableimpression, 'Y') = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_MEASURABLE_IMPS,
                        sum(
                                CASE
                                        WHEN imp.viewableimpression = 'Y' THEN 1
                                        ELSE 0
                                END
                        ) AS TOTAL_VIEWABLE_IMPS,
                        sum(imp.CLICK_COUNT) AS TOTAL_CLICKS,
                        /************** FILLED UNFILLED **************/
                        sum(
                                CASE
                                        WHEN imp.lineitemid > 0
                                        AND imp.creativeid > 0 THEN 1
                                        ELSE 0
                                END
                        ) AS FILLED_IMPS,
                        sum(
                                CASE
                                        WHEN imp.lineitemid = 0
                                        AND imp.creativeid = 0 THEN 1
                                        ELSE 0
                                END
                        ) AS UNFILLED_IMPS,
                        /************** HOUSE IMPS **************/
                        sum(
                                CASE
                                        WHEN ad.company_type = 'HOUSE' THEN 1
                                        ELSE 0
                                END
                        ) AS HOUSE_IMPS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'HOUSE' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS HOUSE_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'PROGRAMATIC' THEN 1
                                        ELSE 0
                                END
                        ) AS PROGRAMATIC_IMPS,
                        sum(
                                CASE
                                        WHEN ad.company_type = 'PROGRAMATIC' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS PROGRAMATIC_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Content Adjacency' THEN 1
                                        ELSE 0
                                END
                        ) AS CC_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Content Adjacency' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CC_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Audience' THEN 1
                                        ELSE 0
                                END
                        ) AS CDT_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Audience' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CDT_CLICKS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Cortex' THEN 1
                                        ELSE 0
                                END
                        ) AS CMT_IMPS,
                        sum(
                                CASE
                                        WHEN ad.productfamily = 'Cortex' THEN imp.CLICK_COUNT
                                        ELSE 0
                                END
                        ) AS CMT_CLICKS
                FROM
                        dfp.impression imp
                        LEFT JOIN azahn.vw_dfp_admanager_v3 ad ON imp.creativeid = ad.creative_id
                        AND imp.lineitemid = ad.lineitem_id
                        INNER JOIN (
                                SELECT
                                        DISTINCT POS
                                FROM
                                        dfp.lkup_position
                                WHERE
                                        DISPLAY = 1
                        ) pos --changed from billable_position table 2/5/18
                        ON imp.pos = pos.pos
                WHERE
                        imp.eventtime BETWEEN :VStartTime
                        AND :VEndTime --imp.eventtime between '2018-08-02 00:00:00' and '2018-08-02 23:59:59'
                        AND imp.env = 0 --and length(imp.requestedadunitsizes) > 5
                GROUP BY
                        pvid,
                        trunc(imp.eventtime),
                        ad.COMPANY_NAME
        ) --order by PVID
        --segmented by hash(PVID) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP1A');

/******************************************************************************************************************************
 
 SUMMARIZE OMNITURE at PVID
 add row numbers OR page view counts at PVID level
 (subject_cd, health_channel_nm, business_reference_cd, site_nm)
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP2 log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_TEMP2' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho TRUNCATE TS7_TEMP2 TRUNCATE TABLE azahn.TS7_TEMP2;

--drop table azahn.TS7_TEMP2 cascade;
\ qecho
INSERT
        TS7_TEMP2 core_page_view --create table azahn.TS7_TEMP2 as(
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP2(
                SELECT
                        *
                FROM
                        (
                                SELECT
                                        pv.PVID,
                                        pv.VISITOR_ID,
                                        pv.VISIT_NUM,
                                        pv.PAGE_VIEW_NUM,
                                        pv.SITE_VISITOR_ID,
                                        pv.ASSET_ID,
                                        pv.PAGE_VIEW_DTM,
                                        pv.SITE_NM,
                                        pv.SUBJECT_CD,
                                        pv.BUSINESS_REFERENCE_CD,
                                        pv.HEALTH_CHANNEL_NM,
                                        pv.P43_GAPFILL AS ECD_VEHICLE_CD,
                                        --added 1/22/18
                                        pv.TRAFFIC_SOURCE,
                                        pv.PAGE_EVENT_CD,
                                        pv.BOARD_IDENTIFIER_CD,
                                        pv.REFERRING_MODULE_ID,
                                        pv.SPONSOR_CLIENT_NM,
                                        --added 10/26/17
                                        pv.SPONSOR_BRAND_NM,
                                        --added 10/26/17
                                        pv.SPONSOR_PROGRAM_NM,
                                        pv.DEVICE_TYPE_NM,
                                        pv.COUNTRY_NM,
                                        pv.GEO_ZIP,
                                        pv.MOBILE_OPTIMIZED_CD,
                                        pv.PAGEVIEWTIMESPENT,
                                        pv.PAGE_LOAD_TIME,
                                        -------------------------- PVID --------------------------
                                        row_number() over (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num,
                                                        pv.page_View_dtm
                                        ) AS PVID_ROW_NUM,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0 THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_VIEWS,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0
                                                        AND pv.board_identifier_cd = 'ab1' THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_AB_PAGE_VIEWS,
                                        sum(pv.PAGEVIEWTIMESPENT) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_TIME_SPENT,
                                        sum(pv.PAGE_LOAD_TIME) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_LOAD_TIME
                                FROM
                                        omniture_new.core_page_View pv
                                WHERE
                                        pv.page_View_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_View_dtm between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'
                                        AND pv.page_Event_Cd = 0
                        ) x
                WHERE
                        x.pvid_row_num = 1
        ) --order by
        --PVID
        --segmented by hash(PVID) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP2');

/******************************************************************************************************************************
 
 SUMMARIZE OMNITURE at PVID
 add row numbers OR page view counts at PVID level
 (subject_cd, health_channel_nm, business_reference_cd, site_nm)
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP2 dedupe cons_network
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP2(
                SELECT
                        *
                FROM
                        (
                                SELECT
                                        pv.PVID,
                                        pv.VISITOR_ID,
                                        pv.VISIT_NUM,
                                        pv.PAGE_VIEW_NUM,
                                        pv.SITE_VISITOR_ID,
                                        pv.ASSET_ID,
                                        pv.PAGE_VIEW_DTM,
                                        (
                                                CASE
                                                        WHEN pv.site_nm IN(
                                                                'core',
                                                                'rxlist',
                                                                'emedicinehealth',
                                                                'medicinenet',
                                                                'onhealth',
                                                                'medterms'
                                                        ) THEN 'cons_network'
                                                        ELSE 'other'
                                                END
                                        ) AS SITE_NM,
                                        --        pv.SITE_NM,
                                        pv.SUBJECT_CD,
                                        pv.BUSINESS_REFERENCE_CD,
                                        pv.HEALTH_CHANNEL_NM,
                                        pv.P43_GAPFILL AS ECD_VEHICLE_CD,
                                        --added 1/22/18
                                        pv.TRAFFIC_SOURCE,
                                        pv.PAGE_EVENT_CD,
                                        pv.BOARD_IDENTIFIER_CD,
                                        pv.REFERRING_MODULE_ID,
                                        pv.SPONSOR_CLIENT_NM,
                                        --added 10/26/17
                                        pv.SPONSOR_BRAND_NM,
                                        --added 10/26/17
                                        pv.SPONSOR_PROGRAM_NM,
                                        pv.DEVICE_TYPE_NM,
                                        pv.COUNTRY_NM,
                                        pv.GEO_ZIP,
                                        pv.MOBILE_OPTIMIZED_CD,
                                        pv.PAGEVIEWTIMESPENT,
                                        pv.PAGE_LOAD_TIME,
                                        -------------------------- PVID --------------------------
                                        row_number() over (
                                                PARTITION by pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num,
                                                        pv.page_View_dtm
                                        ) AS PVID_ROW_NUM,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0 THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_VIEWS,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0
                                                        AND pv.board_identifier_cd = 'ab1' THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_AB_PAGE_VIEWS,
                                        sum(pv.PAGEVIEWTIMESPENT) OVER (
                                                PARTITION by pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_TIME_SPENT,
                                        sum(pv.PAGE_LOAD_TIME) OVER (
                                                PARTITION by pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_LOAD_TIME
                                FROM
                                        omniture_new.core_page_View pv
                                WHERE
                                        pv.page_View_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_View_dtm between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'
                                        AND pv.page_Event_Cd = 0
                                        AND pv.site_nm IN(
                                                'core',
                                                'rxlist',
                                                'emedicinehealth',
                                                'medicinenet',
                                                'onhealth',
                                                'medterms'
                                        )
                        ) x
                WHERE
                        x.pvid_row_num = 1
        ) --order by
        --PVID
        --segmented by hash(PVID) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP2');

/******************************************************************************************************************************
 
 SUMMARIZE OMNITURE at PVID
 PROFESSIONAL
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP2 professional
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP2(
                SELECT
                        *
                FROM
                        (
                                SELECT
                                        pv.PVID,
                                        pv.VISITOR_ID,
                                        pv.VISIT_NUM,
                                        pv.PAGE_VIEW_NUM,
                                        pv.SITE_VISITOR_ID,
                                        pv.ASSET_ID,
                                        pv.PAGE_VIEW_DTM,
                                        pv.SITE_NM,
                                        pv.PAGE_LEAD_CONCEPT_ID :: int,
                                        --int added 10/22/19
                                        NULL AS BUSINESS_REFERENCE_CD,
                                        NULL AS HEALTH_CHANNEL_NM,
                                        --        pv.P43_GAPFILL as ECD_VEHICLE_CD, --added 1/22/18
                                        NULL AS ECD_VEHICLE_CD,
                                        pv.TRAFFIC_SOURCE,
                                        pv.PAGE_EVENT_CD,
                                        pv.AD_BLOCKER_CD,
                                        pv.REFERRING_MODULE_ID,
                                        pv.SUPPORTING_CLIENT_NM,
                                        --changed 9/6/19
                                        pv.SUPPORTED_ACTIVITY,
                                        --changed 9/6/19
                                        pv.SUPPORTED_PRODUCT,
                                        --changed 9/6/19
                                        --        pv.SPONSOR_CLIENT_NM, --added 10/26/17
                                        --        pv.SPONSOR_BRAND_NM,--added 10/26/17
                                        --        pv.SPONSOR_PROGRAM_NM,
                                        pv.DEVICE_TYPE_NM,
                                        pv.COUNTRY_NM,
                                        pv.GEO_ZIP,
                                        pv.MOBILE_OPTIMIZED_CD,
                                        pv.PAGEVIEWTIMESPENT,
                                        pv.PAGE_LOAD_TIME,
                                        -------------------------- PVID --------------------------
                                        row_number() over (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num,
                                                        pv.page_View_dtm
                                        ) AS PVID_ROW_NUM,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0 THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_VIEWS,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0
                                                        AND pv.AD_BLOCKER_CD = 'ab1' THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_AB_PAGE_VIEWS,
                                        sum(pv.PAGEVIEWTIMESPENT) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_TIME_SPENT,
                                        sum(pv.PAGE_LOAD_TIME) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_LOAD_TIME --        from omniture_new.prof_page_View pv --changed 9/6/19
                                FROM
                                        omniture_new.mscp_page_View pv
                                WHERE
                                        pv.page_View_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_View_dtm between '2019-07-30 00:00:00' and '2019-07-30 23:59:59'
                                        AND pv.page_Event_Cd = 0
                        ) x
                WHERE
                        x.pvid_row_num = 1 --select * From (
                        --        select
                        --        pv.PVID,
                        --        pv.VISITOR_ID,
                        --        pv.VISIT_NUM,
                        --        pv.PAGE_VIEW_NUM,
                        --        pv.SITE_VISITOR_ID,
                        --        pv.ASSET_ID,
                        --        pv.PAGE_VIEW_DTM,
                        --        pv.SITE_NM,
                        --        pv.SUBJECT_CD,
                        --        pv.BUSINESS_REFERENCE_CD,
                        --        pv.HEALTH_CHANNEL_NM,
                        ----        pv.P43_GAPFILL as ECD_VEHICLE_CD, --added 1/22/18
                        --        null as ECD_VEHICLE_CD,
                        --        pv.TRAFFIC_SOURCE,
                        --        pv.PAGE_EVENT_CD,
                        --        pv.BOARD_IDENTIFIER_CD,
                        --        pv.REFERRING_MODULE_ID,
                        --        pv.SPONSOR_CLIENT_NM, --added 10/26/17
                        --        pv.SPONSOR_BRAND_NM,--added 10/26/17
                        --        pv.SPONSOR_PROGRAM_NM,
                        --        pv.DEVICE_TYPE_NM,
                        --        pv.COUNTRY_NM,
                        --        pv.GEO_ZIP,
                        --        pv.MOBILE_OPTIMIZED_CD,
                        --        pv.PAGEVIEWTIMESPENT,
                        --        pv.PAGE_LOAD_TIME,
                        --        -------------------------- PVID --------------------------
                        --        row_number() over (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num, pv.page_View_dtm) as PVID_ROW_NUM,
                        --        sum(case when pv.page_event_cd = 0 then 1 else 0 end) OVER (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num DESC, pv.page_View_dtm DESC) as PVID_PAGE_VIEWS,
                        --        sum(case when pv.page_event_cd = 0 and pv.board_identifier_cd = 'ab1' then 1 else 0 end) OVER (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num DESC, pv.page_View_dtm DESC) as PVID_AB_PAGE_VIEWS,
                        --        sum(pv.PAGEVIEWTIMESPENT) OVER (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num DESC, pv.page_View_dtm DESC) as PVID_TIME_SPENT,
                        --        sum(pv.PAGE_LOAD_TIME) OVER (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num DESC, pv.page_View_dtm DESC) as PVID_PAGE_LOAD_TIME
                        --
                        --        from omniture_new.prof_page_View pv
                        --        where
                        --        pv.page_View_dtm between :VStartTime and :VEndTime
                        ----        pv.page_View_dtm between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'
                        --        and pv.page_Event_Cd = 0
                        --)x
                        --where
                        --x.pvid_row_num = 1
        ) --order by
        --PVID
        --segmented by hash(PVID) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP2');

/******************************************************************************************************************************
 
 SUMMARIZE OMNITURE at PVID
 MOBILE APPS
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP2 mobile
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP2(
                SELECT
                        *
                FROM
                        (
                                SELECT
                                        pv.PVID,
                                        pv.VISITOR_ID,
                                        pv.VISIT_NUM,
                                        pv.PAGE_VIEW_NUM,
                                        pv.SITE_VISITOR_ID,
                                        pv.ASSET_ID,
                                        pv.PAGE_VIEW_DTM,
                                        pv.SITE_NM,
                                        pv.SUBJECT_CD,
                                        pv.BUSINESS_REFERENCE_CD,
                                        pv.HEALTH_CHANNEL_NM,
                                        NULL AS ECD_VEHICLE_CD,
                                        --added 1/22/18
                                        pv.TRAFFIC_SOURCE,
                                        pv.PAGE_EVENT_CD,
                                        NULL AS BOARD_IDENTIFIER_CD,
                                        pv.REFERRING_MODULE_ID,
                                        NULL AS SPONSOR_CLIENT_NM,
                                        --added 10/26/17
                                        NULL AS SPONSOR_BRAND_NM,
                                        --added 10/26/17
                                        NULL AS SPONSOR_PROGRAM_NM,
                                        pv.DEVICE_TYPE_NM,
                                        pv.COUNTRY_NM,
                                        pv.GEO_ZIP,
                                        NULL AS MOBILE_OPTIMIZED_CD,
                                        pv.PAGEVIEWTIMESPENT,
                                        pv.PAGE_LOAD_TIME,
                                        -------------------------- PVID --------------------------
                                        row_number() over (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num,
                                                        pv.page_View_dtm
                                        ) AS PVID_ROW_NUM,
                                        sum(
                                                CASE
                                                        WHEN pv.page_event_cd = 0 THEN 1
                                                        ELSE 0
                                                END
                                        ) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_VIEWS,
                                        --        sum(case when pv.page_event_cd = 0 and pv.board_identifier_cd = 'ab1' then 1 else 0 end) OVER (partition by pv.site_nm,pv.visitor_id,pv.visit_num,pv.pvid order by pv.visitor_ID, pv.visit_num, pv.page_View_num DESC, pv.page_View_dtm DESC) as PVID_AB_PAGE_VIEWS,
                                        0 AS PVID_AB_PAGE_VIEWS,
                                        sum(pv.PAGEVIEWTIMESPENT) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_TIME_SPENT,
                                        sum(pv.PAGE_LOAD_TIME) OVER (
                                                PARTITION by pv.site_nm,
                                                pv.visitor_id,
                                                pv.visit_num,
                                                pv.pvid
                                                ORDER BY
                                                        pv.visitor_ID,
                                                        pv.visit_num,
                                                        pv.page_View_num DESC,
                                                        pv.page_View_dtm DESC
                                        ) AS PVID_PAGE_LOAD_TIME
                                FROM
                                        omniture_new.mobile_page_View pv
                                WHERE
                                        pv.page_View_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_View_dtm between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'
                                        AND pv.page_Event_Cd = 0
                        ) x
                WHERE
                        x.pvid_row_num = 1
        ) --order by
        --PVID
        --segmented by hash(PVID) all nodes
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP2');

\ qecho
UPDATE
        TS7_TEMP2 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_TEMP2' AS name,
                        trunc(Page_View_dtm) AS Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_TEMP2
                WHERE
                        trunc(Page_View_dtm) = :VStartTime
                GROUP BY
                        trunc(Page_View_dtm)
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 Join Omniture and DFP data
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_TEMP3 log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_TEMP3' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho TRUNCATE TS7_TEMP3 TRUNCATE TABLE azahn.TS7_TEMP3;

\ qecho
INSERT
        TS7_TEMP3 join_T1_T2 --drop table azahn.TS7_TEMP3 cascade;
        --create table azahn.TS7_TEMP3 as(
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP3(
                SELECT
                        omni.VISITOR_ID,
                        omni.VISIT_NUM,
                        omni.PAGE_VIEW_NUM,
                        omni.SITE_VISITOR_ID,
                        omni.ASSET_ID,
                        omni.PAGE_VIEW_DTM,
                        omni.PVID,
                        omni.SITE_NM,
                        omni.SUBJECT_CD,
                        omni.BUSINESS_REFERENCE_CD,
                        omni.HEALTH_CHANNEL_NM,
                        omni.ECD_VEHICLE_CD,
                        --added 1/22/18
                        omni.TRAFFIC_SOURCE,
                        omni.PAGE_EVENT_CD,
                        omni.BOARD_IDENTIFIER_CD,
                        omni.REFERRING_MODULE_ID,
                        omni.SPONSOR_CLIENT_NM,
                        omni.SPONSOR_BRAND_NM,
                        omni.SPONSOR_PROGRAM_NM,
                        omni.DEVICE_TYPE_NM,
                        omni.COUNTRY_NM,
                        omni.GEO_ZIP,
                        omni.MOBILE_OPTIMIZED_CD,
                        omni.PAGEVIEWTIMESPENT,
                        omni.PAGE_LOAD_TIME,
                        omni.PVID_ROW_NUM,
                        omni.PVID_PAGE_VIEWS,
                        omni.PVID_AB_PAGE_VIEWS,
                        omni.PVID_TIME_SPENT,
                        omni.PVID_PAGE_LOAD_TIME,
                        (
                                CASE
                                        WHEN omni.pvid IS NULL THEN 'NO_OMNI_PVID'
                                        WHEN dfp.pvid IS NULL THEN 'NO_DFP_PVID'
                                        ELSE 'OK'
                                END
                        ) AS PVID_MATCH,
                        dfp.TOTAL_IMPRESSIONS,
                        dfp.TOTAL_ELIGIBLE_IMPS,
                        dfp.TOTAL_MEASURABLE_IMPS,
                        dfp.TOTAL_VIEWABLE_IMPS,
                        dfp.TOTAL_CLICKS,
                        dfp.FILLED_IMPS,
                        dfp.UNFILLED_IMPS,
                        dfp.HOUSE_IMPS,
                        dfp.HOUSE_CLICKS,
                        dfp.PROGRAMATIC_IMPS,
                        dfp.PROGRAMATIC_CLICKS,
                        dfp.CC_IMPS,
                        dfp.CC_CLICKS,
                        dfp.CDT_IMPS,
                        dfp.CDT_CLICKS,
                        dfp.CMT_IMPS,
                        --added 1/22/18
                        dfp.CMT_CLICKS,
                        --added 1/22/18
                        row_number() over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SITE_VISITOR,
                        row_number() over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SITE_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SITE_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SITE_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SUBJECT_CD_VISITOR,
                        row_number() over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SUBJECT_CD_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SUBJECT_CD_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SUBJECT_CD_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS HEALTH_CENTER_VISITOR,
                        row_number() over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS HEALTH_CENTER_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS HEALTH_CENTER_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS HEALTH_CENTER_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS BUSINESS_REFERENCE_VISITOR,
                        row_number() over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS BUSINESS_REFERENCE_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS BUSINESS_REFERENCE_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS BUSINESS_REFERENCE_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_PROGRAM_VISITOR,
                        row_number() over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_PROGRAM_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_PROGRAM_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_PROGRAM_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_BRAND_VISITOR,
                        row_number() over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_BRAND_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_BRAND_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_BRAND_AB_PAGE_VIEWS,
                        ----- added 1/22/18 -----
                        row_number() over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ECD_VISITOR,
                        --added 1/22/18
                        row_number() over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ECD_VISIT,
                        --added 1/22/18
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ECD_PAGE_VIEWS,
                        --added 1/22/18
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ECD_AB_PAGE_VIEWS,
                        --added 1/22/18
                        sc.F_0_17,
                        sc.F_18_24,
                        sc.F_25_34,
                        sc.F_35_44,
                        sc.F_45_54,
                        sc.F_55_64,
                        sc.F_65_PLUS,
                        sc.F_TOTAL,
                        sc.M_0_17,
                        sc.M_18_24,
                        sc.M_25_34,
                        sc.M_35_44,
                        sc.M_45_54,
                        sc.M_55_64,
                        sc.M_65_PLUS,
                        sc.M_TOTAL
                FROM
                        azahn.TS7_TEMP2 omni
                        LEFT JOIN azahn.TS7_TEMP1 dfp ON omni.pvid = dfp.PVID
                        LEFT JOIN(
                                SELECT
                                        pv.visitor_ID,
                                        pv.visit_num,
                                        trunc(pv.page_View_dtm) AS PAGE_VIEW_DT,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd IN(
                                                                'seg_sc-coresc_f-0-2',
                                                                'seg_sc-coresc_f-7-12',
                                                                'seg_sc-coresc_f-13-17'
                                                        ) THEN pv.VISITOR_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_0_17,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-18-24' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_18_24,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-25-34' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_25_34,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-35-44' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_35_44,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-45-54' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_45_54,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-55-64' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_55_64,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-Over 65' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_65_PLUS,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd ilike 'seg_sc-coresc_f%' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_TOTAL,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd IN(
                                                                'seg_sc-coresc_m-0-2',
                                                                'seg_sc-coresc_m-7-12',
                                                                'seg_sc-coresc_m-13-17'
                                                        ) THEN pv.VISITOR_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_0_17,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-18-24' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_18_24,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-25-34' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_25_34,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-35-44' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_35_44,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-45-54' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_45_54,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-55-64' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_55_64,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-Over 65' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_65_PLUS,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd ilike 'seg_sc-coresc_m%' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_TOTAL
                                FROM
                                        omniture_new.core_page_View pv
                                WHERE
                                        pv.page_view_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_view_dtm between '2017-07-30 00:00:00' and '2017-07-30 23:59:59'        
                                        AND pv.application_segmentation_cd ilike 'seg_sc%'
                                GROUP BY
                                        pv.visitor_ID,
                                        pv.visit_num,
                                        trunc(pv.page_View_dtm)
                        ) sc ON omni.VISITOR_ID = sc.VISITOR_ID
                        AND omni.visit_num = sc.visit_num
                        AND trunc(omni.PAGE_VIEW_DTM) = sc.PAGE_VIEW_DT
        ) --order by
        --VISITOR_ID,
        --VISIT_NUM,
        --PAGE_VIEW_NUM
        --SEGMENTED BY hash(VISITOR_ID) ALL NODES
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP3');

\ qecho
UPDATE
        TS7_TEMP3 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_TEMP3' AS name,
                        trunc(Page_View_dtm) AS Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN PVID_page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_TEMP3
                WHERE
                        trunc(Page_View_dtm) = :VStartTime
                GROUP BY
                        trunc(Page_View_dtm)
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 Join Omniture and DFP data for advertiser
 added 8/3/18
 
 ******************************************************************************************************************************/
\ qecho TRUNCATE TS7_TEMP3A TRUNCATE TABLE azahn.TS7_TEMP3A;

\ qecho
INSERT
        TS7_TEMP3A join_T1_T2
INSERT
        /*+ direct */
        INTO azahn.TS7_TEMP3A(
                SELECT
                        omni.VISITOR_ID,
                        omni.VISIT_NUM,
                        omni.PAGE_VIEW_NUM,
                        omni.SITE_VISITOR_ID,
                        omni.ASSET_ID,
                        omni.PAGE_VIEW_DTM,
                        omni.PVID,
                        omni.SITE_NM,
                        omni.SUBJECT_CD,
                        omni.BUSINESS_REFERENCE_CD,
                        omni.HEALTH_CHANNEL_NM,
                        omni.ECD_VEHICLE_CD,
                        --added 1/22/18
                        dfp.COMPANY_NAME,
                        --added 8/3/18
                        omni.TRAFFIC_SOURCE,
                        omni.PAGE_EVENT_CD,
                        omni.BOARD_IDENTIFIER_CD,
                        omni.REFERRING_MODULE_ID,
                        omni.SPONSOR_CLIENT_NM,
                        omni.SPONSOR_BRAND_NM,
                        omni.SPONSOR_PROGRAM_NM,
                        omni.DEVICE_TYPE_NM,
                        omni.COUNTRY_NM,
                        omni.GEO_ZIP,
                        omni.MOBILE_OPTIMIZED_CD,
                        omni.PAGEVIEWTIMESPENT,
                        omni.PAGE_LOAD_TIME,
                        omni.PVID_ROW_NUM,
                        omni.PVID_PAGE_VIEWS,
                        omni.PVID_AB_PAGE_VIEWS,
                        omni.PVID_TIME_SPENT,
                        omni.PVID_PAGE_LOAD_TIME,
                        (
                                CASE
                                        WHEN omni.pvid IS NULL THEN 'NO_OMNI_PVID'
                                        WHEN dfp.pvid IS NULL THEN 'NO_DFP_PVID'
                                        ELSE 'OK'
                                END
                        ) AS PVID_MATCH,
                        dfp.TOTAL_IMPRESSIONS,
                        dfp.TOTAL_ELIGIBLE_IMPS,
                        dfp.TOTAL_MEASURABLE_IMPS,
                        dfp.TOTAL_VIEWABLE_IMPS,
                        dfp.TOTAL_CLICKS,
                        dfp.FILLED_IMPS,
                        dfp.UNFILLED_IMPS,
                        dfp.HOUSE_IMPS,
                        dfp.HOUSE_CLICKS,
                        dfp.PROGRAMATIC_IMPS,
                        dfp.PROGRAMATIC_CLICKS,
                        dfp.CC_IMPS,
                        dfp.CC_CLICKS,
                        dfp.CDT_IMPS,
                        dfp.CDT_CLICKS,
                        dfp.CMT_IMPS,
                        --added 1/22/18
                        dfp.CMT_CLICKS,
                        --added 1/22/18
                        row_number() over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SITE_VISITOR,
                        row_number() over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SITE_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SITE_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SITE_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SUBJECT_CD_VISITOR,
                        row_number() over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SUBJECT_CD_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SUBJECT_CD_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SUBJECT_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SUBJECT_CD_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS HEALTH_CENTER_VISITOR,
                        row_number() over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS HEALTH_CENTER_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS HEALTH_CENTER_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.HEALTH_CHANNEL_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS HEALTH_CENTER_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS BUSINESS_REFERENCE_VISITOR,
                        row_number() over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS BUSINESS_REFERENCE_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS BUSINESS_REFERENCE_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.BUSINESS_REFERENCE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS BUSINESS_REFERENCE_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_PROGRAM_VISITOR,
                        row_number() over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_PROGRAM_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_PROGRAM_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_PROGRAM_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_PROGRAM_AB_PAGE_VIEWS,
                        row_number() over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_BRAND_VISITOR,
                        row_number() over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS SPONSOR_BRAND_VISIT,
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_BRAND_PAGE_VIEWS,
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.SPONSOR_BRAND_NM,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS SPONSOR_BRAND_AB_PAGE_VIEWS,
                        ----- added 1/22/18 -----
                        row_number() over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ECD_VISITOR,
                        --added 1/22/18
                        row_number() over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ECD_VISIT,
                        --added 1/22/18
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ECD_PAGE_VIEWS,
                        --added 1/22/18
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by omni.ECD_VEHICLE_CD,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ECD_AB_PAGE_VIEWS,
                        --added 1/22/18
                        ----- added 8/03/18 -----
                        row_number() over (
                                PARTITION by DFP.COMPANY_NAME,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ADVERTISER_VISITOR,
                        ----- added 8/03/18
                        row_number() over (
                                PARTITION by dfp.COMPANY_NAME,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM
                        ) AS ADVERTISER_VISIT,
                        ----- added 8/03/18
                        sum(omni.PVID_PAGE_VIEWS) over (
                                PARTITION by dfp.COMPANY_NAME,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ADVERTISER_PAGE_VIEWS,
                        ----- added 8/03/18
                        sum(omni.PVID_AB_PAGE_VIEWS) over (
                                PARTITION by dfp.COMPANY_NAME,
                                omni.SITE_NM,
                                trunc(omni.PAGE_VIEW_DTM),
                                omni.VISITOR_ID,
                                omni.VISIT_NUM
                                ORDER BY
                                        omni.PAGE_VIEW_NUM DESC
                        ) AS ADVERTISER_AB_PAGE_VIEWS,
                        ----- added 8/03/18
                        sc.F_0_17,
                        sc.F_18_24,
                        sc.F_25_34,
                        sc.F_35_44,
                        sc.F_45_54,
                        sc.F_55_64,
                        sc.F_65_PLUS,
                        sc.F_TOTAL,
                        sc.M_0_17,
                        sc.M_18_24,
                        sc.M_25_34,
                        sc.M_35_44,
                        sc.M_45_54,
                        sc.M_55_64,
                        sc.M_65_PLUS,
                        sc.M_TOTAL
                FROM
                        azahn.TS7_TEMP2 omni
                        INNER JOIN azahn.TS7_TEMP1A dfp ON omni.pvid = dfp.PVID
                        LEFT JOIN(
                                SELECT
                                        pv.visitor_ID,
                                        pv.visit_num,
                                        trunc(pv.page_View_dtm) AS PAGE_VIEW_DT,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd IN(
                                                                'seg_sc-coresc_f-0-2',
                                                                'seg_sc-coresc_f-7-12',
                                                                'seg_sc-coresc_f-13-17'
                                                        ) THEN pv.VISITOR_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_0_17,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-18-24' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_18_24,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-25-34' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_25_34,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-35-44' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_35_44,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-45-54' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_45_54,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-55-64' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_55_64,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_f-Over 65' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_65_PLUS,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd ilike 'seg_sc-coresc_f%' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS F_TOTAL,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd IN(
                                                                'seg_sc-coresc_m-0-2',
                                                                'seg_sc-coresc_m-7-12',
                                                                'seg_sc-coresc_m-13-17'
                                                        ) THEN pv.VISITOR_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_0_17,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-18-24' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_18_24,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-25-34' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_25_34,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-35-44' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_35_44,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-45-54' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_45_54,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-55-64' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_55_64,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd = 'seg_sc-coresc_m-Over 65' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_65_PLUS,
                                        count(
                                                DISTINCT CASE
                                                        WHEN pv.application_segmentation_cd ilike 'seg_sc-coresc_m%' THEN pv.visitor_ID
                                                        ELSE NULL
                                                END
                                        ) AS M_TOTAL
                                FROM
                                        omniture_new.core_page_View pv
                                WHERE
                                        pv.page_view_dtm BETWEEN :VStartTime
                                        AND :VEndTime --        pv.page_View_dtm between '2018-08-02 00:00:00' and '2018-08-02 23:59:59'
                                        AND pv.application_segmentation_cd ilike 'seg_sc%'
                                GROUP BY
                                        pv.visitor_ID,
                                        pv.visit_num,
                                        trunc(pv.page_View_dtm)
                        ) sc ON omni.VISITOR_ID = sc.VISITOR_ID
                        AND omni.visit_num = sc.visit_num
                        AND trunc(omni.PAGE_VIEW_DTM) = sc.PAGE_VIEW_DT
        ) --order by
        --VISITOR_ID,
        --VISIT_NUM,
        --PAGE_VIEW_NUM
        --SEGMENTED BY hash(VISITOR_ID) ALL NODES
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_TEMP3A');

/******************************************************************************************************************************
 
 insert into output table
 SITE_NM
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 SITE_NM log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT SITE' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 SITE_NM
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'SITE_NM' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        NULL AS SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.SITE_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.SITE_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SITE_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.site_visit = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        --t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

\ qecho
UPDATE
        TS7_TEMP1 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT SITE' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'SITE_NM'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 SUBJECT_CD
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 SUBJECT_CD log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT SUBJECT' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 SUBJECT_CD
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'SUBJECT_CD' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        t.SUBJECT_CD,
                        NULL AS SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.SUBJECT_CD_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SUBJECT_CD_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        nvl(t.subject_cd, 99999) AS SUBJECT_CD,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.subject_Cd_visit = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND nvl(t.subject_cd, 99999) = vis.subject_cd
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        t.SUBJECT_CD,
                        --t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

\ qecho
UPDATE
        TS7_TEMP1 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT SUBJECT' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'SUBJECT_CD'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 SUBJECT_CD
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 HEALTH_CENTER log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT HC' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 HEALTH_CENTER
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'HEALTH_CENTER' AS REPORT_LEVEL,
                        t.SITE_NM,
                        t.HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        NULL AS SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.HEALTH_CENTER_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.HEALTH_CENTER_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        nvl(t.HEALTH_CHANNEL_NM, 'ntc') AS HEALTH_CHANNEL_NM,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.HEALTH_CENTER_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND nvl(t.HEALTH_CHANNEL_NM, 'ntc') = vis.HEALTH_CHANNEL_NM
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        --t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

\ qecho
UPDATE
        TS7_OUTPUT HC log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT HC' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'HEALTH_CENTER'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 BUSINESS_REFERENCE_CD
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 BUSINESS_REFERENCE_CD log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT CC' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 BUSINESS_REFERENCE_CD
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'BUSINESS_REFERENCE_CD' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        t.BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        NULL AS SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.BUSINESS_REFERENCE_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.BUSINESS_REFERENCE_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        nvl(t.BUSINESS_REFERENCE_CD, 'ntc') AS BUSINESS_REFERENCE_CD,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.BUSINESS_REFERENCE_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND nvl(t.BUSINESS_REFERENCE_CD, 'ntc') = vis.BUSINESS_REFERENCE_CD
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        --t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

\ qecho
UPDATE
        TS7_OUTPUT CC log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT CC' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'BUSINESS_REFERENCE_CD'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 SPONSOR_PROGRAM_NM
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 SPONSOR_PROGRAM_NM log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT SP' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 SPONSOR_PROGRAM_NM
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'SPONSOR_PROGRAM_NM' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        t.SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.SPONSOR_PROGRAM_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_PROGRAM_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        nvl(t.SPONSOR_PROGRAM_NM, 'ntc') AS SPONSOR_PROGRAM_NM,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.SPONSOR_PROGRAM_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND nvl(t.SPONSOR_PROGRAM_NM, 'ntc') = vis.SPONSOR_PROGRAM_NM
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_OUTPUT_V1');

\ qecho
UPDATE
        TS7_OUTPUT SP log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT SP' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'SPONSOR_PROGRAM_NM'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 SPONSOR_BRAND_NM
 added 10/26/17
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 SPONSOR_BRAND_NM log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT BRAND' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 SPONSOR_BRAND_NM
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'SPONSOR_BRAND_NM' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        t.SPONSOR_CLIENT_NM || '_' || t.SPONSOR_BRAND_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.SPONSOR_BRAND_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.SPONSOR_BRAND_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        nvl(t.SPONSOR_BRAND_NM, 'ntc') AS SPONSOR_BRAND_NM,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.SPONSOR_BRAND_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND nvl(t.SPONSOR_BRAND_NM, 'ntc') = vis.SPONSOR_BRAND_NM
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        t.SPONSOR_CLIENT_NM || '_' || t.SPONSOR_BRAND_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_OUTPUT_V1');

\ qecho
UPDATE
        TS7_OUTPUT BRAND log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT BRAND' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'SPONSOR_BRAND_NM'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 ECD_VEHICLE_CD
 added 1/23/18
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 ECD_VEHICLE_CD log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT ECD' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 ECD_VEHICLE_CD
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                --drop table azahn.TS7_OUTPUT_V1 cascade;
                --create table azahn.TS7_OUTPUT_V1 as(
                SELECT
                        'ECD_CODE' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        NULL AS SPONSOR_PROGRAM_NM,
                        t.ECD_VEHICLE_CD,
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.ECD_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.ECD_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.ECD_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3 t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        t.ECD_VEHICLE_CD,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3 t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.ECD_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND t.ECD_VEHICLE_CD = vis.ECD_VEHICLE_CD
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        --t.SUBJECT_CD,
                        t.ECD_VEHICLE_CD,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

SELECT
        analyze_statistics('azahn.TS7_OUTPUT_V1');

\ qecho
UPDATE
        TS7_OUTPUT ECD_VEHICLE_CD log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT ECD' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'ECD_CODE'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

/******************************************************************************************************************************
 
 insert into output table
 ADVERTISER
 added 8/3/18
 
 ******************************************************************************************************************************/
\ qecho
INSERT
        TS7_OUTPUT_V1 COMPANY_NAME log
INSERT INTO
        azahn.TS_log(
                SELECT
                        'TS7_OUTPUT ADVERTISER' AS TABLE,
                        :VStartTime AS load_date,
                        NULL AS Total_Records_Inserted,
                        NULL Core_Page_Views,
                        NULL AS ConsNetwork_Page_Views,
                        NULL AS ProfNetwork_Page_Views,
                        sysdate AS start_Time,
                        NULL AS end_Time
                FROM
                        dual
        );

COMMIT;

\ qecho
INSERT
        TS7_OUTPUT_V1 COMPANY_NAME
INSERT
        /*+ direct */
        INTO azahn.TS7_OUTPUT_V1(
                SELECT
                        'ADVERTISER' AS REPORT_LEVEL,
                        t.SITE_NM,
                        NULL AS HEALTH_CHANNEL_NM,
                        NULL AS BUSINESS_REFERENCE_CD,
                        NULL AS SUBJECT_CD,
                        t.COMPANY_NAME AS SPONSOR_PROGRAM_NM,
                        NULL AS ECD_VEHICLE_CD,
                        --added 1/22/18
                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM,
                        --t.PVID_MATCH,
                        count(
                                DISTINCT CASE
                                        WHEN t.ADVERTISER_VISITOR = 1 THEN t.VISITOR_ID
                                        ELSE NULL
                                END
                        ) AS VISITORS,
                        count(
                                DISTINCT CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN t.visitor_ID || T.visit_num
                                        ELSE NULL
                                END
                        ) AS VISITS,
                        sum(t.PVID_PAGE_VIEWS) AS PAGE_VIEWS,
                        sum(t.PVID_AB_PAGE_VIEWS) AS AB_PAGE_VIEWS,
                        sum(
                                CASE
                                        WHEN t.PVID_MATCH = 'OK' THEN t.PVID_PAGE_VIEWS
                                        ELSE 0
                                END
                        ) AS DFP_PAGE_VIEWS,
                        sum(t.PVID_TIME_SPENT) AS TIME_SPENT,
                        sum(t.PVID_PAGE_LOAD_TIME) AS PAGE_LOAD_TIME,
                        sum(t.TOTAL_IMPRESSIONS) AS TOTAL_IMPRESSIONS,
                        sum(t.TOTAL_ELIGIBLE_IMPS) AS TOTAL_ELIGIBLE_IMPS,
                        sum(t.TOTAL_MEASURABLE_IMPS) AS TOTAL_MEASURABLE_IMPS,
                        sum(t.TOTAL_VIEWABLE_IMPS) AS TOTAL_VIEWABLE_IMPS,
                        sum(t.TOTAL_CLICKS) AS TOTAL_CLICKS,
                        sum(t.FILLED_IMPS) AS FILLED_IMPS,
                        sum(t.UNFILLED_IMPS) AS UNFILLED_IMPS,
                        sum(t.HOUSE_IMPS) AS HOUSE_IMPS,
                        sum(t.HOUSE_CLICKS) AS HOUSE_CLICKS,
                        sum(t.PROGRAMATIC_IMPS) AS PROGRAMATIC_IMPS,
                        sum(t.PROGRAMATIC_CLICKS) AS PROGRAMATIC_CLICKS,
                        sum(t.CC_IMPS) AS CC_IMPS,
                        sum(t.CC_CLICKS) AS CC_CLICKS,
                        sum(t.CDT_IMPS) AS CDT_IMPS,
                        sum(t.CDT_CLICKS) AS CDT_CLICKS,
                        sum(t.CMT_IMPS) AS CMT_IMPS,
                        sum(t.CMT_CLICKS) AS CMT_CLICKS,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_0_17
                                        ELSE 0
                                END
                        ) AS F_0_17,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_18_24
                                        ELSE 0
                                END
                        ) AS F_18_24,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_25_34
                                        ELSE 0
                                END
                        ) AS F_25_34,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_35_44
                                        ELSE 0
                                END
                        ) AS F_35_44,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_45_54
                                        ELSE 0
                                END
                        ) AS F_45_54,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_55_64
                                        ELSE 0
                                END
                        ) AS F_55_64,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_65_PLUS
                                        ELSE 0
                                END
                        ) AS F_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN F_TOTAL
                                        ELSE 0
                                END
                        ) AS F_TOTAL,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_0_17
                                        ELSE 0
                                END
                        ) AS M_0_17,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_18_24
                                        ELSE 0
                                END
                        ) AS M_18_24,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_25_34
                                        ELSE 0
                                END
                        ) AS M_25_34,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_35_44
                                        ELSE 0
                                END
                        ) AS M_35_44,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_45_54
                                        ELSE 0
                                END
                        ) AS M_45_54,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_55_64
                                        ELSE 0
                                END
                        ) AS M_55_64,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_65_PLUS
                                        ELSE 0
                                END
                        ) AS M_65_PLUS,
                        sum(
                                CASE
                                        WHEN t.ADVERTISER_VISIT = 1 THEN M_TOTAL
                                        ELSE 0
                                END
                        ) AS M_TOTAL
                FROM
                        azahn.TS7_TEMP3A t
                        INNER JOIN(
                                SELECT
                                        t.SITE_NM,
                                        t.VISITOR_ID,
                                        t.VISIT_NUM,
                                        --        nvl(t.subject_cd,99999) as SUBJECT_CD,
                                        t.COMPANY_NAME,
                                        trunc(t.PAGE_VIEW_DTM) AS PAGE_VIEW_DT,
                                        ref.TRAFFIC_SOURCE_CLASS,
                                        ref.TRAFFIC_SOURCE_GROUP,
                                        t.TRAFFIC_SOURCE,
                                        (
                                                CASE
                                                        WHEN t.COUNTRY_NM = 'usa' THEN 'US'
                                                        ELSE 'xUS'
                                                END
                                        ) AS COUNTRY,
                                        t.DEVICE_TYPE_NM
                                FROM
                                        azahn.TS7_TEMP3A t
                                        LEFT JOIN azahn.TS_SOURCE_REFERENCE2 ref ON t.TRAFFIC_SOURCE = ref.TRAFFIC_SOURCE_NM
                                        AND t.SITE_NM = ref.site_nm --        where         t.visitor_ID = 19983578638
                                WHERE
                                        t.ADVERTISER_VISIT = 1
                        ) vis ON t.SITE_NM = vis.SITE_NM
                        AND t.VISITOR_ID = vis.VISITOR_ID
                        AND t.VISIT_NUM = vis.VISIT_NUM
                        AND t.COMPANY_NAME = vis.COMPANY_NAME
                        AND trunc(t.PAGE_VIEW_DTM) = vis.PAGE_VIEW_DT
                GROUP BY
                        t.SITE_NM,
                        --t.HEALTH_CHANNEL_NM,
                        --t.BUSINESS_REFERENCE_CD,
                        t.COMPANY_NAME,
                        --t.SPONSOR_PROGRAM_NM,
                        trunc(t.PAGE_VIEW_DTM),
                        vis.TRAFFIC_SOURCE_CLASS,
                        vis.TRAFFIC_SOURCE_GROUP,
                        vis.TRAFFIC_SOURCE,
                        vis.COUNTRY,
                        vis.DEVICE_TYPE_NM
        ) --order by
        --PAGE_VIEW_DT
        --segmented by hash(REPORT_LEVEL, PAGE_VIEW_DT, SITE_NM) all nodes;
;

COMMIT;

\ qecho
UPDATE
        TS7_TEMP1 log
UPDATE
        AZAHN.TS_log
SET
        Records = n.counts,
        End_Time = n.End_Time,
        core_page_Views = n.Core_PV,
        consnetwork_page_Views = n.ConsNetwork_PV,
        profnetwork_page_Views = n.ProfNetwork_PV
FROM
        (
                SELECT
                        'TS7_OUTPUT ADVERTISER' AS name,
                        Page_View_dt,
                        count(*) AS counts,
                        sum(
                                CASE
                                        WHEN site_Nm = 'core' THEN page_Views
                                        ELSE 0
                                END
                        ) AS Core_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'core',
                                                'medicinenet',
                                                'emedicinehealth',
                                                'medterms',
                                                'rxlist',
                                                'onhealth'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ConsNetwork_PV,
                        suM(
                                CASE
                                        WHEN site_nm IN(
                                                'medscape',
                                                'emedicine',
                                                'cme',
                                                'mscp',
                                                'mdedge',
                                                'medscape',
                                                'medscape-fr',
                                                'medscape-de',
                                                'medscape-es',
                                                'medscape-pt'
                                        ) THEN page_Views
                                        ELSE 0
                                END
                        ) AS ProfNetwork_PV,
                        Sysdate AS Start_Time,
                        Sysdate AS End_Time
                FROM
                        azahn.TS7_OUTPUT_V1
                WHERE
                        Page_View_dt = :VStartTime
                        AND REPORT_LEVEL = 'ADVERTISER'
                GROUP BY
                        Page_View_dt
        ) n
WHERE
        Load_Date = n.Page_View_dt
        AND tablename = n.name
        AND records IS NULL;

COMMIT;

TRUNCATE TABLE azahn.TS7_TEMP1;

TRUNCATE TABLE azahn.TS7_TEMP1A;

TRUNCATE TABLE azahn.TS7_TEMP2;

TRUNCATE TABLE azahn.TS7_TEMP3;

TRUNCATE TABLE azahn.TS7_TEMP3A;

\ o