/****************************************************** 
 * MODIFY THIS SECTION TO SET UP A NEW GA4 PROPERTY
*******************************************************/
const custom_event_params = [
  { param: 'eventAction', type: 'string' },
  { param: 'eventCategory', type: 'string' },
  { param: 'login_state', type: 'string' },
  { param: 'loginValue', type: 'int' },
  { param: 'om_campaign_id', type: 'string' },
  { param: 'om_campaign_name', type: 'string' },
  { param: 'om_dest', type: 'string' },
  { param: 'post_author', type: 'string' },
  { param: 'post_date', type: 'string' },
  { param: 'post_first_category', type: 'string' },
  { param: 'post_id', type: 'int' },
  { param: 'post_type', type: 'string' },
  { param: 'post_type2', type: 'string' },
  { param: 'pvid', type: 'int' },
  { param: 'registrationValue', type: 'int' },
  { param: 'site_nm', type: 'string' },
  { param: 'zone', type: 'string' },
  { param: 'container_id', type: 'string' }
];

const custom_user_params = [
  { param: 'visitor_type', type: 'string' }
];

const conversion_event_names = [
  "file_download",
  "sponsor_link_click",
  "ad_click",
  "ad_impression",
  "external_link_click",
  "click",
  "cookie_notice_impression",
  "user_registration"
];

/******************************************************/

/**
 * Get date in format: YYYYMMDD
 * @params {integer} prior_days
 */
const get_date = (prior_days) => {
  let d = new Date();
  d.setDate(d.getDate()-prior_days);
  let mm = d.getMonth() + 1;
  let dd = d.getDate();
  return String([d.getFullYear(),(mm>9 ? '' : '0') + mm,(dd>9 ? '' : '0') + dd].join(''));
}

// Get date ranges
//
const prior_days = parseInt(dataform.projectConfig.vars.prior_days)
const range_end = get_date(1);
const range_start = get_date(1+prior_days);
const attribution_start = get_date(31+prior_days)

// Export project vars
module.exports = { 
    range_start,
    range_end,
    attribution_start,
    custom_event_params,
    conversion_event_names,
    custom_user_params
};