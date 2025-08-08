// This is an example of how a custom channel grouping might be applied to the "ga4_int_sessions" view
// ${channel_grouping.defaultGrouping('last_non_direct_attribution.session_manual_source','last_non_direct_attribution.session_manual_medium','slast_non_direct_attribution.session_manual_campaign_name')} AS session_default_channel_grouping,

const isEventConversion = () => {
    return `
        case 
            when event_name in ('${project_variables.conversion_event_names.join("', '")}')
                then STRUCT(5 as event_value_in_usd, event_name as goal)
            when event_name = 'optinmonster_conversion' AND REGEXP_CONTAINS(form_name, '(enl|newsletter)')
                then STRUCT(5 as event_value_in_usd, "newsletter_signup" as goal)
            when event_name = 'scroll' AND percent_scrolled IN (25, 50, 75, 100)
                then STRUCT(10 as event_value_in_usd, CONCAT("scroll_", percent_scrolled) as goal)

            -- scroll 90?  eg > 25
            -- Primary Line Item Imp	event_name = page_View AND GAM.lineitem_name contains "primary" 
            -- Primary Line Item Click	event_name = page_View AND GAM.lineitem_name contains "primary"
            -- event_name = aim_signal and AIM_hcp_type=<use AIM Value Lookup>
            -- 2nd Page View / Bounce Rate
        end
    `;
};

module.exports = { isEventConversion };


