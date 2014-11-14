package io.tradle.loader;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;
import io.tradle.Config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Sharable
public class BitLoaderHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final String CONFIG_PATH = "conf/config.json";
  private static final Object WEBCLASS_PARENT_FOLDER = "parentFolder";
  private static final Object WEBCLASS_NAME = "name";
  private static final Object WEBCLASS_COMMENT = "comment";
  
  private static int HTTP_CONFLICT = 409;
  
  private static String APP_TYPE = "http://www.hudsonfog.com/voc/model/social/App";
  private static String APP_PATH = "appPath";
  private static String APP_NAME = "name";
  private static String APP_TITLE = "title";

  private static String WEB_DESIGNER_PACKAGE = "http://www.hudsonfog.com/voc/system/designer/";
  private static String WEBCLASS_TYPE    = WEB_DESIGNER_PACKAGE + "WebClass";
  private static String WEBPROPERTY_TYPE = WEB_DESIGNER_PACKAGE + "WebProperty";
  private static String WEBPROPERTY_RANGE_URI = "rangeUri";
  private static String STRING_PROPERTY_TYPE   = WEB_DESIGNER_PACKAGE + "StringProperty";
  private static String INTEGER_PROPERTY_TYPE  = WEB_DESIGNER_PACKAGE + "IntegerProperty";
  private static String BOOLEAN_PROPERTY_TYPE  = WEB_DESIGNER_PACKAGE + "BooleanProperty";
  private static String DOUBLE_PROPERTY_TYPE   = WEB_DESIGNER_PACKAGE + "DoubleProperty";
  private static String FLOAT_PROPERTY_TYPE    = WEB_DESIGNER_PACKAGE + "FloatProperty";
  private static String DATE_PROPERTY_TYPE     = WEB_DESIGNER_PACKAGE + "DateProperty";
  private static String LONG_PROPERTY_TYPE     = WEB_DESIGNER_PACKAGE + "LongProperty";
  private static String IMAGE_PROPERTY_TYPE    = WEB_DESIGNER_PACKAGE + "ImageProperty";
  private static String RESOURCE_PROPERTY_TYPE = WEB_DESIGNER_PACKAGE + "ResourceProperty";
  private static String BACKLINK_PROPERTY_TYPE = WEB_DESIGNER_PACKAGE + "BacklinkProperty"; // property items of some container type
  private static String INLINE_PROPERTY_TYPE   = WEB_DESIGNER_PACKAGE + "InlineProperty"; // like Money {value, currency}
  HashMap<String, String> rangeToType = new HashMap();

  private static String MONEY_TYPE = "http://www.hudsonfog.com/voc/model/company/Money";
  private static String STRING_CLASS_URI;

  private static HashMap<String, JSONObject> propertyTypeToResource = new HashMap(); // map of property type to property metadata

  private Config config;
  
  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) {
    try {
      readHelper(ctx, req);
    } catch (Exception e) {
      e = e;
    } finally {
      writeResponse(ctx, req);
    }
  }
  
  private void readHelper(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
    QueryStringDecoder qs = new QueryStringDecoder(req.getUri());
    Map<String, List<String>> parameters = qs.parameters();
    if (parameters.size() == 0)
      return;
    String json = parameters.get("json").get(0);
    Gson gson = new GsonBuilder().create();
    try {
      this.config = gson.fromJson(new BufferedReader(new FileReader(CONFIG_PATH)), Config.class);
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("couldn't find config file at path: " + CONFIG_PATH);
    } 
    String type = null;
    JSONObject jo = null;
    jo = new JSONObject(json);
    if (!jo.isNull("type"))
      type = jo.getString("type");
    String msg = null;
    try {
      if (type == null)
        createResource(jo.getString("_type"), jo, req);
      else
        createModel(type, jo, req);
      req.setDecoderResult(DecoderResult.SUCCESS);
    } catch (Exception e) {
      msg = e.getMessage();
      req.setDecoderResult(DecoderResult.failure(e));
    }
    
  }
  /**
   * @param type - type of the resource that is being created as it is presented in JSON 'business.coffeeShop.Order'
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the resource
   * @param req
   * @param res
   * @return     - uri of the created resource
   * @throws JSONException
   * @throws UnsupportedEncodingException 
   */
  private String createResource(String type, JSONObject jo, HttpRequest req) throws JSONException, UnsupportedEncodingException {
    String typeServerName = config.serverAlias();
    int idx = type.lastIndexOf(".");
    String appPath = Character.toUpperCase(type.charAt(0)) + type.substring(1, idx).replace(".", "_");
    
    String rType = type.startsWith("http://") ? type : "http://" + typeServerName + "/voc/dev/" + appPath + "/" + type.substring(idx + 1);
    // Check the app exists
    JSONObject rjo = getResource(APP_TYPE, APP_PATH + "=" + appPath);
    if (rjo == null)
      throw new IllegalStateException("Resource '" + appPath + "' does not exist");
//    String appUri = rjo.getString("_uri");
    return createResource(rType, appPath, jo, req);
  }
  private JSONObject getResource(String type, String... params) {
    try {
      JSONArray ja = getResources(type, params);
      return ja == null ? null : ja.getJSONObject(0);
    } catch (JSONException e) {
      return null;
    }
  }
  
  private JSONArray getResources(String type, String... params) {
    StringBuilder sb = new StringBuilder();
    sb.append("http://").append(config.serverName()).append("/api/v1/").append(type).append("?");
    for (String p: params) {
      sb.append(p);
      sb.append("&");
    }
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(sb.toString()).openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(10000);
      conn.setRequestProperty("User-Agent", "Mozilla/4.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");
      conn.setInstanceFollowRedirects(false);
      int code = conn.getResponseCode();
      BufferedReader bin = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      sb.setLength(0);
      while (true) {
        String s = bin.readLine();
        if (s == null)
          break;
        sb.append(s);
      }
      
      return new JSONArray(sb.toString());
    } catch (IOException | JSONException e) {
      return null;
    } finally {
      conn.disconnect();
    }
  }
  private String mkResource(String type, String... params) throws JSONException {
    StringBuilder sb = new StringBuilder();
    sb.append("http://").append(config.serverName()).append("/api/v1/m/").append(type);
    String uri = sb.toString();
    sb.setLength(0);
    for (String p: params) {
      sb.append(p);
      sb.append("&");
    }
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) new URL(uri).openConnection();
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(10000);
      conn.setRequestProperty("User-Agent", "Mozilla/4.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");
      conn.setDoInput(true);
      conn.setDoOutput(true);
      OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
      writer.write(sb.toString());
      writer.flush();
      int code = conn.getResponseCode();
      if (code == HTTP_CONFLICT) {
        JSONObject jo = getResource(type, params);
        return jo.isNull("_uri") ? null : jo.getString("_uri");
      }
      else
        return conn.getHeaderField("Location");
    } catch (IOException e) {
      return null;
    } finally {
      conn.disconnect();
    }
  }
  /**
   * @param type - type of the resource that is being created (like http://tradle.io/voc/dev/business/coffeeShop/Order)
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the resource
   * @param req
   * @param res
   * @return     - uri of the created resource
   * @throws JSONException
   * @throws UnsupportedEncodingException 
   */
  private String createResource(String rType, String appPath, JSONObject jo, HttpRequest req) throws JSONException, UnsupportedEncodingException {
    JSONObject modelJO = getResource(WEBCLASS_TYPE, "davClassUri=" + rType + "&$publish=y");
    if (modelJO == null)
      return null;

    JSONArray propsJO = null;
    
    try {
      propsJO = getResources(WEBPROPERTY_TYPE, "domain=" + URLEncoder.encode(modelJO.getString("_uri"), "UTF-8"));
    } catch (UnsupportedEncodingException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    int n = propsJO.length();
    HashMap<String, JSONObject> props = new HashMap();
    for (int i=0; i<n; i++) {
      JSONObject p = propsJO.getJSONObject(i);
      String key = p.getString("name");
      props.put(key,  p);
    }

    StringBuilder sb = new StringBuilder();
    Iterator<String> it = jo.keys();
    while (it.hasNext()) {
      String pName = it.next();
      if (props.get(pName) == null  ||  jo.isNull(pName))
        continue;
      JSONObject meta = props.get(pName);
      String propertyUri = meta.getString("_uri");
      int idx = propertyUri.indexOf("?");
      String propertyTypeShort = propertyUri.substring(propertyUri.lastIndexOf("/") + 1, idx);
      String range = meta.getString("rangeUri");

      sb.append(pName);
      sb.append("=");
      switch (propertyTypeShort) {
      case "BooleanProperty":
        sb.append(jo.getBoolean(pName));
        break;
      case "StringProperty":
        sb.append(URLEncoder.encode(jo.getString(pName), "UTF-8"));
        break;
      case "IntegerProperty":
        sb.append(jo.getInt(pName));
        break;
      case "FloatProperty":
      case "DoubleProperty":
        sb.append(jo.getDouble(pName));
        break;
      case "DateProperty":
      case "LongProperty":
        sb.append(jo.getLong(pName));
        break;
      case "InlineProperty":
        Object o = jo.get(pName);
        if (o instanceof String) { // like 0.001BTC
          String v = (String)o;
          if (range.endsWith("Money")) {
            int len = v.length();
            int i = 0;
            StringBuilder valsb = new StringBuilder();
            for (; i<len; i++) {
              char ch = v.charAt(i);
              if (Character.isDigit(ch) || ch == '.')
                valsb.append(ch);
              else if (ch == ',')
                continue;
              else
                break;
            }
              
            sb.append(URLEncoder.encode("{ value: " + valsb +  (i == len ? "" : ", currency: " + v.substring(i).trim()) + " }", "UTF-8"));
            break;
          }
        }
        JSONObject ires = (JSONObject)o;
        sb.append(URLEncoder.encode(ires.toString(), "UTF-8"));
        break;
      case "ResourceProperty":
        o = jo.get(pName);
        if (!(o instanceof JSONObject))
          break;
        JSONObject rres = jo.getJSONObject(pName);
        String rUri = createResource(rres.getString(WEBPROPERTY_TYPE + "/" + WEBPROPERTY_RANGE_URI), appPath, rres, req);
        sb.append(URLEncoder.encode(rUri, "UTF-8"));
        break;
      }
      sb.append("&");
    }
    sb.append("&-app=" + appPath);
    return mkResource(rType, sb.toString());
  }

  /**
   * @param type - type of the new model.
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the model
   * @param req
   * 
   * @throws JSONException
   * @throws UnsupportedEncodingException 
   */
  private String createModel(String type, JSONObject jo, HttpRequest req) throws JSONException, UnsupportedEncodingException {
    int idx = type.lastIndexOf(".");
    String appPath = Character.toUpperCase(type.charAt(0)) + type.substring(1, idx).replace(".", "_");

    JSONObject appJO = getResource(APP_TYPE, APP_PATH + "=" + appPath);
    String appUri = null;
    if (appJO == null) {
      StringBuilder sb = new StringBuilder();
      sb.append("appPath=").append(appPath);
      sb.append("&title=").append(URLEncoder.encode(appPath.replace("_", " ")));
      String[] a = appPath.split("\\.");
      StringBuilder sb1 = new StringBuilder();
      for (String aa: a)
        sb1.append(Character.toUpperCase(aa.charAt(0))).append(aa.substring(1));
      sb.append("&name=").append(sb1);
      appUri = mkResource(APP_TYPE, sb.toString());
    }
    else
      appUri = appJO.getString("_uri");

    if (appUri == null)
      return null;
    boolean isPrimitive = jo.isNull("properties");
    String sub = null;
    if (isPrimitive) {
      if (STRING_CLASS_URI == null) {
        JSONObject subJO = getResource(WEBCLASS_TYPE, "$index=y&davClassUri=http://www.hudsonfog.com/voc/system/String");
        if (subJO != null) 
          STRING_CLASS_URI = subJO.getString("_uri");  
      }
      sub = STRING_CLASS_URI;
       
    }  
    StringBuilder sb = new StringBuilder();
    sb.append(WEBCLASS_PARENT_FOLDER).append("=").append(URLEncoder.encode(appUri, "UTF-8") + "&");
    sb.append(WEBCLASS_NAME + "=" + type.substring(idx + 1) + "&");
    if (sub != null)
      sb.append("subClassOf=" + URLEncoder.encode(sub, "UTF-8") + "&");
    try {
      String comment = jo.getString("comment");
      sb.append(WEBCLASS_COMMENT + "=" + URLEncoder.encode(comment, "UTF-8") + "&");
    } catch (JSONException e) {
      
    }
    try {
      String previousVersion = jo.getString("_previous_version");
      sb.append("previousVersion=" + previousVersion + "&");      
    } catch (JSONException e) {
      
    }
    try {
      String originalVersion = jo.getString("_original_version");
      sb.append("originalVersion=" + originalVersion + "&");      
    } catch (JSONException e) {
      
    }
    String wcUri = mkResource(WEBCLASS_TYPE, sb.toString());
    if (wcUri == null || isPrimitive)
      return null;
    JSONObject props = jo.getJSONObject("properties");
    Iterator<String> it = props.keys();
    
    String serverName = config.serverName();
    String typeServerName = config.serverAlias();
    String wcMoney = null;
    HashMap<String, String> h = new HashMap();
    sb.setLength(0);
    while (it.hasNext()) {
      String propName = it.next();
      JSONObject annotations = props.getJSONObject(propName);
      String range = null;
      try {
        range = annotations.getString("range");
      } catch (JSONException e) {
        e = e;
      }
      // Assume that if no range then it's a string
      String propertyType = null;
      String backlink = null;
      String iRange = null;
      if (range == null) 
        propertyType = STRING_PROPERTY_TYPE;
      else {
        if (!annotations.isNull("backLink"))
          backlink = annotations.getString("backLink");
        if (backlink != null) { 
          String rUri = "http://" + typeServerName + "/voc/dev/" + range.replace(".", "/");
          iRange = h.get(rUri);
          if (iRange == null) {
            JSONObject rTypeJO = getResource(WEBCLASS_TYPE, "davClassUri=" + rUri); //????
            
            if (rTypeJO == null) { 
              iRange = createModelForBacklink(appUri, rUri, type, wcUri, annotations, req);
              if (iRange == null)
                throw new IllegalStateException("Model " + range + " does not exist");
              h.put(rUri, iRange);
            }
          }
          propertyType = BACKLINK_PROPERTY_TYPE;
        }
        else if (range.endsWith(".Money")) {
          propertyType = INLINE_PROPERTY_TYPE;
          if (wcMoney == null) {
            JSONObject moneyJO = getResource(WEBCLASS_TYPE, "davClassUri=" + MONEY_TYPE); //????
            wcMoney = moneyJO.getString("_uri");
          }
          iRange = wcMoney;
//          if (currency != null)
//            p.setString(InlineProperty._currency, currency);
        }
        else {
          propertyType = getPropertyType(range);
          if (propertyType == null) { 
            String rUri = "http://" + typeServerName + "/voc/dev/" + range.replace(".", "/");
            iRange = h.get(rUri);
            if (iRange == null) {
              JSONObject rTypeJO = getResource(WEBCLASS_TYPE, "davClassUri=" + rUri); //????
              
              if (rTypeJO == null) 
                throw new IllegalStateException("Model " + range + " does not exist");
              
              iRange = rTypeJO.getString("_uri");
              if (rTypeJO.isNull("subClassOf"))
                propertyType = RESOURCE_PROPERTY_TYPE;
              else {
                if (rTypeJO.getString("subClassOfUri").endsWith("String"))
                  propertyType = STRING_PROPERTY_TYPE;
                else
                  propertyType = STRING_PROPERTY_TYPE;
              }

              h.put(rUri, iRange);
              rangeToType.put(iRange, propertyType);
            }
            else 
              propertyType = rangeToType.get(iRange);
          }
        }
      }
      sb.setLength(0);
      if (iRange != null)
        sb.append("range=" + URLEncoder.encode(iRange, "UTF-8") + "&");
      sb.append("name=" + propName);
      int length = propName.length();
      sb.append("&label=");
      String s = "";
      s += Character.toUpperCase(propName.charAt(0));
      for (int i=1; i<length; i++) {
        char ch = propName.charAt(i);
        if (ch == '_') {
          s += " ";
          continue;
        }
        if (Character.isUpperCase(ch)) 
          s += " ";
        s += ch; 
      }
      sb.append(URLEncoder.encode(s));
      sb.append("&domain=" + URLEncoder.encode(wcUri, "UTF-8"));
      
      JSONObject propertyTypeMeta = propertyTypeToResource.get(propertyType);
      if (propertyTypeMeta == null) { 
        propertyTypeMeta = getResource(WEBCLASS_TYPE, "davClassUri=" + propertyType);
        propertyTypeToResource.put(propertyType, propertyTypeMeta);
      }
      Iterator<String> propAnnotations = annotations.keys();
      while (propAnnotations.hasNext()) {
        String annotation = propAnnotations.next();
        if (annotation.equals("range")  ||  annotation.equals("domain"))
          continue;
        if (propertyTypeMeta.isNull(annotation))
          continue;
        Object aVal = null;
        if (!annotations.isNull(annotation))
          aVal = annotations.get(annotation);
        if (aVal != null)
          sb.append(annotation + "=" + URLEncoder.encode(aVal.toString(), "UTF-8"));
      }
      mkResource(propertyType, sb.toString());
    }
    return wcUri;
  }
  
  /**
   * @param type - type of the new model.
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the model
   * @param req
   * 
   * @throws JSONException
   * @throws UnsupportedEncodingException 
   */
  private String createModelForBacklink(String appUri
                                      , String type
                                      , String parentType
                                      , String parentWcUri
                                      , JSONObject jo
                                      , HttpRequest req) throws JSONException, UnsupportedEncodingException {
    int idx = type.lastIndexOf("/");
    StringBuilder sb = new StringBuilder();
    sb.append(WEBCLASS_PARENT_FOLDER).append("=").append(URLEncoder.encode(appUri, "UTF-8") + "&");
    sb.append(WEBCLASS_NAME + "=" + type.substring(idx + 1) + "&");
    try {
      String previousVersion = jo.getString("_previous_version");
      sb.append("previousVersion=" + previousVersion + "&");      
    } catch (JSONException e) {
      
    }
    try {
      String originalVersion = jo.getString("_original_version");
      sb.append("originalVersion=" + originalVersion + "&");      
    } catch (JSONException e) {
      
    }
    String wcUri = mkResource(WEBCLASS_TYPE, sb.toString());
    if (wcUri == null)
      return null;
    
    String propName = jo.getString("backLink");
    String iRange = parentWcUri;
    String propertyType = RESOURCE_PROPERTY_TYPE;
    
    sb.setLength(0);
    sb.append("range=" + URLEncoder.encode(iRange, "UTF-8") + "&");
    sb.append("name=" + propName);
    int length = propName.length();
    sb.append("&label=");
    String s = Character.toUpperCase(propName.charAt(0)) + "";
    for (int i=1; i<length; i++) {
      char ch = propName.charAt(i);
      if (ch == '_') {
        s += " ";
        continue;
      }
      if (Character.isUpperCase(ch)) 
        s += " ";
      s += ch; 
    }
    sb.append(URLEncoder.encode(s));
    sb.append("&domain=" + URLEncoder.encode(wcUri, "UTF-8"));
    
    JSONObject propertyTypeMeta = propertyTypeToResource.get(propertyType);
    if (propertyTypeMeta == null) { 
      propertyTypeMeta = getResource(WEBCLASS_TYPE, "davClassUri=" + propertyType);
      propertyTypeToResource.put(propertyType, propertyTypeMeta);
    }
    mkResource(propertyType, sb.toString());
  
    return wcUri;
  }

  /**
   * Maps the range of the property to the internal object that represents this range 
   * 
   * @param range - type of the property 
   * @return - internal object representing this range
   */
  private String getPropertyType(String range) {
    switch (range) {
    case "int":
      return INTEGER_PROPERTY_TYPE;
    case "boolean":
      return BOOLEAN_PROPERTY_TYPE;
    case "date":
      return DATE_PROPERTY_TYPE;
    case "float":
      return FLOAT_PROPERTY_TYPE;
    case "bigdecimal":
      return DOUBLE_PROPERTY_TYPE;
    case "biginteger":
        return STRING_PROPERTY_TYPE;
    case "long":
      return LONG_PROPERTY_TYPE;
    case "image":
      return IMAGE_PROPERTY_TYPE;
    default:
      return null;
    }
  }
  private boolean writeResponse(ChannelHandlerContext ctx, HttpRequest req) {
    System.out.println("WRITING REPSONSE FOR REQUEST: " + req.getUri());
    DecoderResult dr = req.getDecoderResult();
    boolean isSuccess = dr.isSuccess();
    String msg = dr.toString();
      
    FullHttpResponse response = new DefaultFullHttpResponse(
            HTTP_1_1, isSuccess ? OK : BAD_REQUEST,
            Unpooled.copiedBuffer(msg, CharsetUtil.UTF_8));

    response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    return false;
  }
}