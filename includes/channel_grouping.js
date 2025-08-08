// This is an example of how a custom channel grouping might be applied to the "ga4_int_sessions" view
// ${channel_grouping.defaultGrouping('last_non_direct_attribution.session_manual_source','last_non_direct_attribution.session_manual_medium','slast_non_direct_attribution.session_manual_campaign_name')} AS session_default_channel_grouping,

const defaultGrouping = (session_source,session_medium,session_campaign) => {
    return 
        `case 
            when (session_source = 'direct' or session_source = '(direct)' or session_source is null) 
                and (regexp_contains(session_medium, r'^(\(not set\)|\(none\))$') or session_medium is null) 
                then 'Direct'
            when regexp_contains(session_campaign, 'cross-network') then 'Cross-network'
            when (regexp_contains(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
                or regexp_contains(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$'))
                and regexp_contains(session_medium, '^(.*cp.*|ppc|paid.*)$') then 'Paid Shopping'
            when regexp_contains(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
                and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Search'
            when regexp_contains(session_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
                and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Social'
            when regexp_contains(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
                and regexp_contains(session_medium,'^(.*cp.*|ppc|paid.*)$') then 'Paid Video'
            when session_medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
            when regexp_contains(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
                or regexp_contains(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
            when regexp_contains(session_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
                or session_medium in ('social','social-network','social-media','sm','social network','social media') then 'Organic Social'
            when regexp_contains(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
                or regexp_contains(session_medium,'^(.*video.*)$') then 'Organic Video'
            when regexp_contains(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
                or session_medium = 'organic' then 'Organic Search'
            when regexp_contains(session_source,'email|e-mail|e_mail|e mail')
                or regexp_contains(session_medium,'email|e-mail|e_mail|e mail') then 'Email'
            when session_medium = 'affiliate' then 'Affiliates'
            when session_medium = 'referral' then 'Referral'
            when session_medium = 'audio' then 'Audio'
            when session_medium = 'sms' then 'SMS'
            when session_medium like '%push'
                or regexp_contains(session_medium,'mobile|notification') then 'Mobile Push Notifications'
            else 'Unassigned'
        end`
}

module.exports = { defaultGrouping };

/* ALTERNATE VERSION

// TODO: Setting gclid to null because it is not currently available when the function is called in the ga4_int_sessions table

const defaultGrouping = (source,medium,campaign,gclid) => {
    gclid = null;
    return `
    case
          when regexp_contains(${campaign}, r'^(.*shop|shopping.*)$') 
              and regexp_contains(${medium}, r'^(.*cp.*|ppc|paid.*)$')
              then 'shopping_paid'
          when regexp_contains(${source}, r'^(twitter|facebook|fb|instagram|ig|linkedin|pinterest).*$')
              and regexp_contains(${medium}, r'^(.*cp.*|ppc|paid.*|social_paid)$') 
              then 'social_paid'
          when regexp_contains(${source}, r'^(youtube).*$')
              and regexp_contains(${medium}, r'^(.*cp.*|ppc|paid.*)$') 
              then 'video_paid'
          when regexp_contains(${medium}, r'^(display|banner|expandable|interstitial|cpm)$') 
              then 'display'
          when regexp_contains(${source}, r'^(google|bing).*$') 
              and regexp_contains(${medium}, r'^(.*cp.*|ppc|paid.*)$') or
              ${gclid} is not null
              then 'search_paid'
          when regexp_contains(${medium}, r'^(.*cp.*|ppc|paid.*)$') 
              then 'other_paid'
          when regexp_contains(${medium}, r'^(.*shop|shopping.*)$') 
              then 'shopping_organic'
          when regexp_contains(${source}, r'^.*(twitter|t\.co|facebook|instagram|linkedin|lnkd\.in|pinterest).*') 
              or regexp_contains(${medium}, r'^(social|social_advertising|social-advertising|social_network|social-network|social_media|social-media|sm|social-unpaid|social_unpaid)$') 
              then 'social_organic'
          when regexp_contains(${medium}, r'^(.*video.*)$') 
              then 'video_organic'
          when regexp_contains(${source}, r'^(google|bing|yahoo|baidu|duckduckgo|yandex|ask)$') 
              or ${medium} = 'organic'
              then 'search_organic'
          when regexp_contains(${source}, r'^(email|mail|e-mail|e_mail|e mail|mail\.google\.com)$') 
              or regexp_contains(${medium}, r'^(email|mail|e-mail|e_mail|e mail)$') 
              then 'email'
          when regexp_contains(${medium}, r'^(affiliate|affiliates)$') 
              then 'affiliate'
          when ${medium} = 'referral'
              then 'referral'
          when ${medium} = 'audio' 
              then 'audio'
          when ${medium} = 'sms'
              then 'sms'
          when ends_with(${medium}, 'push')
              or regexp_contains(${medium}, r'.*(mobile|notification).*') 
              then 'mobile_push'
          when (${source} = 'direct' or ${source} is null) 
              and (regexp_contains(${medium}, r'.*(not set|none).*') or ${medium} is null) 
              then 'direct'
          else '(other)'
      end`
  }
*/
