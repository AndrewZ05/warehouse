/**
 * Generate subquery to extract an event parameter from the raw GA4 source tables
 * @params {string} eventParamName
 * @params {string} eventParamType
 */
const eventParam = (eventParamName, eventParamType = "string", eventParamColumnName, arrayField = "event_params") => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    case "float":
      eventParamTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT value.${eventParamTypeName} FROM UNNEST(${arrayField}) WHERE key = '${eventParamName}') AS ${eventParamColumnName || eventParamName}`;
};

const userProp = (userPropName,userPropType = "string") => {
  let userPropTypeName = "";
  switch (userPropType) {
    case "string":
      userPropTypeName = "string_value";
      break;
    case "int":
      userPropTypeName = "int_value";
      break;
    case "double":
      userPropTypeName = "double_value";
      break;
    case "float":
      userPropTypeName = "float_value";
      break;
    default:
      throw "propertyType is not valid";
  }
  return `ARRAY_AGG((SELECT value.${userPropTypeName} AS ${userPropName} FROM UNNEST(user_properties) WHERE key = '${userPropName}') IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[SAFE_OFFSET(0)] AS ${userPropName}`;
};

module.exports = { 
  eventParam,
  userProp 
};
