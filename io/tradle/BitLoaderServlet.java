package io.tradle;

import hudsonfog.voc.model.company.Money;
import hudsonfog.voc.model.social.App;
import hudsonfog.voc.system.designer.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.fogx.webdav.*;
import com.fogx.webdav.util.DavResourceSupport;
import com.fogx.webdav.util.UrlUtil;

public class BitLoaderServlet implements Servlet {
  ServletContext ctx;
  transient ServletConfig config;
  
  public void init(ServletConfig config) throws ServletException, UnavailableException {
    this.config = config;
    ctx = config.getServletContext();
  }
  public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
    String json = req.getParameter("json");
    if (json == null)
      return;
    try {
      JSONObject jo = new JSONObject(json);
      String type = null;
      try {
        type = jo.getString("type");
      } catch (JSONException e) {
        
      }
      if (type == null)
        createResource(jo.getString("_type"), jo, req, res);
      else
        createModel(type, jo, req, res);
    } catch (Exception e) {
      e = e;
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
   */
  private String createResource(String type, JSONObject jo, ServletRequest req, ServletResponse res) throws JSONException {
    String serverName = req.getServerName();
    String typeServerName = serverName.replace("dev.", "");

    String rType = type.startsWith("http://") ? type : "http://" + typeServerName + "/voc/dev/" + type.replace(".", "/");
    String appUri = null;
    int idx = type.lastIndexOf(".");
    String appPath = type.substring(0, idx);
    DavRequest dreq = Dav.getDav().list(App.davClass);
    dreq.setString(App._appPath, appPath);
    try {
      appUri = dreq.execute().getResourceList().get(0).getUri();
    } catch (DavException e) {
      
    }
    return createResource(rType, appUri, jo, req, res);
  }
  /**
   * @param type - type of the resource that is being created (like http://tradle.io/voc/dev/business/coffeeShop/Order)
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the resource
   * @param req
   * @param res
   * @return     - uri of the created resource
   * @throws JSONException
   */
  private String createResource(String rType, String appUri, JSONObject jo, ServletRequest req, ServletResponse res) throws JSONException {
    DavClass rCl = DavClass.getDavClass(rType);
    // Check the range of the property is valid. 
    if (rCl == null) {
      publish(appUri, rType, req);
      rCl = DavClass.getDavClass(rType);
      if (rCl == null)
        return null;
    }
    DavRequest rreq = Dav.getDav().mkresource(rCl);

    Iterator<String> it = jo.keys();
    while (it.hasNext()) {
      String pName = it.next();
      DavProperty dProp = rCl.getProperty(pName);
      if (dProp == null)
        continue;
      DavClass pCl = dProp.getRange();
      if (pCl == null) {
        String rangeUri = dProp.toResource().getResourceUri(DavProperty._range);
        publish(appUri, rangeUri, req);
        rCl = DavClass.getDavClass(rangeUri);
//        pCl = getClass(rangeUri, app, vocLoader, req, res);
      }
      boolean isInlined = pCl.isAlwaysInlined();
      if (pCl.isPrimitive())  
        rreq.setObject(dProp.getUri(), jo.getString(pName));
      else if (isInlined) {
        DavResource ires = new DavResourceSupport();
        DavProperty vProp = pCl.getValueProperty();
        
        ires.setObject(vProp.getUri(), jo.get(pName));
        if (Money.davClass.isAssignableFrom(pCl)) {
          try {
            String cur = jo.getString("currency");
            ires.setResourceUri(Money._currency, cur);
          } catch (JSONException e) {
            
          }
        }
        
        rreq.setInlineResource(dProp.getUri(), ires);
      }
      else {
        JSONObject pjo = jo.getJSONObject(pName);
        String rUri = createResource(pCl.getUri(), appUri, pjo, req, res);
        rreq.setResourceUri(dProp.getUri(), rUri);
      }
    }
    try {
      return rreq.execute().getHeader("Location");
    } catch (DavException e) {
      return null;
    }
  }

  /**
   * @param type - type of the new model.
   * @param appUri - uri of the application in which resource is created (such as coffeshop, SPA, taxes etc.)
   * @param jo - JSON object that contains the body of the model
   * @param req
   * @param res
   * 
   * @throws JSONException
   */
  private void createModel(String type, JSONObject jo, ServletRequest req, ServletResponse res) throws JSONException, ServletException {
    int idx = type.lastIndexOf(".");
    String appPath = type.substring(0, idx);
    
    DavRequest areq = Dav.getDav().list(App.davClass);
    areq.setString(App._appPath, appPath);
    String aName = appPath; //appPath.substring(idx0 + 1);
    String appUri = null;
    try {
      DavResponse ares = areq.execute();
      appUri = ares.getResourceList().get(0).getUri();
    } catch (DavException e) {
      areq = Dav.getDav().mkresource(App.davClass);
      areq.setString(App._title, Character.toUpperCase(aName.charAt(0)) + aName.substring(1).replace(".", " "));
      String[] a = aName.split("\\.");
      StringBuilder sb = new StringBuilder();
      for (String aa: a)
        sb.append(Character.toUpperCase(aa.charAt(0))).append(aa.substring(1));
      areq.setString(App._name, sb.toString());
      
      areq.setString(App._appPath, appPath);
      try {
        appUri = areq.execute().getHeader("Location");
      } catch (DavException ee) {
        return;
      }
    }
    DavRequest dreq = Dav.getDav().mkresource(WebClass.davClass);
    dreq.setResourceUri(WebClass._parentFolder, appUri);
    dreq.setString(WebClass._name, Character.toLowerCase(type.charAt(idx + 1)) + type.substring(idx + 2));
    try {
      String comment = jo.getString("comment");
      if (comment != null)
        dreq.setString(WebClass._comment, comment);
    } catch (JSONException e) {
      
    }
    try {
      String prevVersion = jo.getString("_previous_version");
      dreq.setString(WebClass._previousVersion, prevVersion);
    } catch (JSONException e) {
      
    }
    try {
      String origVersion = jo.getString("_original_version");
      dreq.setString(WebClass._originalVersion, origVersion);
    } catch (JSONException e) {
      
    }
    String wcUri = null;
    try {
      wcUri = dreq.execute().getHeader("Location");
    } catch (DavException e) {
      if (e.getErrorCode() == HttpServletResponse.SC_CONFLICT) 
        wcUri = e.getDavResponse().getHeader("Location");
    }
    if (wcUri == null)
      return;
//    DavResource webClass = null;
//    try {
//      webClass = Dav.getDav().propfind(wcUri).execute().getResource();
//    } catch (DavException e) {
//      
//    }
    JSONObject props = jo.getJSONObject("properties");
    Iterator<String> it = props.keys();
    String serverName = req.getServerName();
    String typeServerName = serverName.replace("dev.", "");
    String wcMoney = null;
    HashMap<String, String> h = new HashMap();
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
      DavClass pCl = null;
      String backlink = null;
      String iRange = null;
      if (range == null) 
        pCl = StringProperty.davClass;
      else {
        try {
          backlink = annotations.getString("backlink");
        } catch (JSONException e) {
          
        }
        if (backlink != null) 
          pCl = BacklinkProperty.davClass;
        else if (range.endsWith(".Money")) {
          pCl = InlineProperty.davClass;
          if (wcMoney == null) {
            DavRequest dr = Dav.getDav(ctx.getInitParameter("IndexServerName")).list(WebClass.davClass);
            dr.setString(WebClass._davClassUri, Money._rdfType);
            try {
              wcMoney = dr.execute().getResourceList().get(0).getUri();
            } catch (DavException e) {
              e = e;
            }
          }
          iRange = wcMoney;
//          if (currency != null)
//            p.setString(InlineProperty._currency, currency);
        }
        else {
          pCl = getPropertyType(range);
          if (pCl == null) { 
            pCl = ResourceProperty.davClass;
            String rUri = "http://" + typeServerName + "/voc/dev/" + range.replace(".", "/");
            
   
            iRange = h.get(rUri);
            if (iRange == null) {
              DavRequest dr = Dav.getDav(serverName).list(WebClass.davClass);
              dr.setString(WebClass._davClassUri, rUri);
              try {
                iRange = dr.execute().getResourceList().get(0).getUri();
                h.put(rUri, iRange);
              } catch (DavException e) {
                // throw Exception that range cl is not known      
                throw new ServletException("The model: " + range + " does not exist");
              }
            }
  
          }
        }
      }
      DavRequest preq = Dav.getDav().mkresource(pCl);
      if (iRange != null)
        preq.setResourceUri(ResourceProperty._range, iRange);
      preq.setString(pCl.getProperty(WebProperty._name).getUri(), propName);
      preq.setString(pCl.getProperty(WebProperty._label).getUri(), propName.replaceAll("_", " "));
      
      preq.setResourceUri(pCl.getProperty(WebProperty._domain).getUri(), wcUri);
      Iterator<String> propAnnotations = annotations.keys();
      while (propAnnotations.hasNext()) {
        String annotation = propAnnotations.next();
        if (annotation.equals("range")  ||  annotation.equals("domain"))
          continue;
        DavProperty aProp = pCl.getProperty(annotation);
        if (aProp == null)
          continue;
        Object aVal = null;
        try {
          aVal = annotations.get(annotation);
        } catch (Exception e) {
          e = e;
        }
        if (aVal != null) 
          preq.setString(aProp.getUri(), aVal.toString());
      }
      try {
        preq.execute();
      } catch (DavException e) {
        e = e;
      }
    }
    publish(appUri, type, req);
  }
  

  /**
   * Maps the range of the property to the internal object that represents this range 
   * 
   * @param range - type of the property 
   * @return - internal object representing this range
   */
  private DavClass getPropertyType(String range) {
    switch (range) {
    case "int":
      return IntegerProperty.davClass;
    case "boolean":
      return BooleanProperty.davClass;
    case "date":
      return DateProperty.davClass;
    case "float":
      return FloatProperty.davClass;
    case "bigdecimal":
      return DoubleProperty.davClass;
    case "biginteger":
        return StringProperty.davClass;
    case "long":
      return LongProperty.davClass;
    case "image":
      return ImageProperty.davClass;
    default:
      return null;
    }
  }
  /**
   * Verifies the validity of the new type and its properties and creates internal representation of this type
   * 
   * @param appUri
   * @param type
   * @param req
   * @return
   */
  private DavClass publish(String appUri, String type, ServletRequest req) {
    String serverName = req.getServerName();
    String typeServerName = serverName.replace("dev.", "");
    StringBuilder sb = new StringBuilder();
    sb.append("http://").append(serverName).append("/proppatch?publish=y&uri=");
    UrlUtil.encode(appUri, sb);
    try {
      Dav.getDav().get(sb.toString()).execute();
    } catch (DavException e) {
      
    }
    String wcType = type.startsWith("http://") ? type : "http://" + typeServerName + "/voc/dev/" + type.replace(".", "/");
    return DavClass.getDavClass(wcType);

  }

  public void destroy() {
  }

  public ServletConfig getServletConfig() {
    if (config == null)
      throw new IllegalStateException("servletConfig is null, make sure you called super.init(servletConfig) in init method of your servlet");
    return config;
  }
  public String getServletInfo() {
    return "Bit Loader: creates models and resources from chain in Local DB";
  }
}