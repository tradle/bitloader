package io.tradle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.fog.rdf.datasource.VocLoader;
import com.fog.webdav.IncrementalPublishing;
import com.fog.webdav.WebDavUtils;
import com.fogx.service.HostAlias;
import com.fogx.webdav.*;
import com.fogx.webdav.server.*;
import com.fogx.webdav.util.DavResourceSupport;

import hudsonfog.voc.model.company.Money;
import hudsonfog.voc.model.social.App;
import hudsonfog.voc.system.designer.*;

public class BitLoaderServlet extends DavServlet {
  HostAlias hostAlias;
  ServletContext ctx;
  
  public void init(ServletConfig config) throws ServletException, UnavailableException {
    super.init(config);
    ctx = getServletContext();
    hostAlias = (HostAlias)ctx.getAttribute(HostAlias.class.getName() + ".class");
    if (hostAlias == null)
      throw new IllegalStateException("service \"" + HostAlias.class.getName() + "\" not found");
    ctx = getServletContext();
  }
  public void service(DavServletRequest req, DavServletResponse res) throws ServletException, IOException {
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
      
      if (type == null)
        createResource(jo.getString("_type"), jo, req, res);
      else
        createModel(type, jo, req, res);
    } catch (Exception e) {
      e = e;
    }  
  }
  private void createResource(String type, JSONObject jo, DavServletRequest req, DavServletResponse res) throws JSONException {
    String d = hostAlias.getDirByHostUrl(ctx.getInitParameter("ServerName"));
    String rType = hostAlias.getAliasByHost(d) + "/voc/dev/" + type.replace(".", "/");

    DavClass rCl = DavClass.getDavClass(rType);
    DavResource app = null;
    VocLoader vocLoader = null;
    if (rCl == null) {
      int idx = type.lastIndexOf(".");
      String appPath = type.substring(0, idx);
      DavRequest dreq = Dav.getDav().list(App.davClass);
      dreq.setString(App._appPath, appPath);
      try {
        app = dreq.execute().getResourceList().get(0);
      } catch (DavException e) {
        
      }
      try {
        vocLoader = new VocLoader(true);
      } catch (Exception e) {
        
      }

      rCl = getClass(rType, app, vocLoader, req, res);
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
        pCl = getClass(rangeUri, app, vocLoader, req, res);
      }
      boolean isInlined = pCl.isAlwaysInlined();
      if (pCl.isPrimitive()  ||  !isInlined) 
        WebDavUtils.setValue(rreq, dProp.getUri(), jo.getString(pName));
      else if (isInlined) {
        DavResource ires = new DavResourceSupport();
        DavProperty vProp = pCl.getValueProperty();
        
        WebDavUtils.setValue(ires, vProp.getUri(), jo.get(pName));
        if (Money.davClass.isAssignableFrom(pCl)) {
          try {
            String cur = jo.getString("currency");
            ires.setResourceUri(Money._currency, cur);
          } catch (JSONException e) {
            
          }
        }
        rreq.setInlineResource(dProp.getUri(), ires);
      }
    }
    try {
      rreq.execute();
    } catch (DavException e) {
      
    }
  }
  private DavClass getClass(String rType, DavResource app, VocLoader vocLoader, DavServletRequest req, DavServletResponse res) {
    DavRequest dreq = Dav.getDav().list(WebClass.davClass);
    dreq.setString(WebClass._davClassUri, rType);
    DavResource webClass = null;
    try {
      webClass = dreq.execute().getResourceList().get(0);
      loadWebClass(app, webClass, vocLoader, req, res);
      return DavClass.getDavClass(rType);
    } catch (DavException e) {
      e = e;
      return null;
    }

  }
  private void createModel(String type, JSONObject jo, DavServletRequest req, DavServletResponse res) throws JSONException, ServletException {
//    String wcType = "http://" + hostAlias.getAliasByHost(getServletContext().getInitParameter("ServerName")) + type.replace(".", "/");
//    dreq.setString(WebClass._davClassUri, wcType);
    int idx = type.lastIndexOf(".");
    String appPath = type.substring(0, idx);
    int idx0 = appPath.lastIndexOf(".");
    
    DavRequest areq = Dav.getDav().list(App.davClass);
    areq.setString(App._appPath, appPath);
    String aName = appPath.substring(idx0 + 1);
    areq.setString(App._name, aName);
    String aUri = null;
    try {
      DavResponse ares = areq.execute();
      aUri = ares.getResourceList().get(0).getUri();
    } catch (DavException e) {
      areq = Dav.getDav().mkresource(App.davClass);
      areq.setString(App._name, aName);
      areq.setString(App._title, Character.toUpperCase(aName.charAt(0)) + aName.substring(1));
      
      areq.setString(App._appPath, appPath);
      try {
        aUri = areq.execute().getHeader("Location");
      } catch (DavException ee) {
        return;
      }
    }
    DavRequest dreq = Dav.getDav().mkresource(WebClass.davClass);
    dreq.setResourceUri(WebClass._parentFolder, aUri);
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
    DavResource webClass = null;
    try {
      webClass = Dav.getDav().propfind(wcUri).execute().getResource();
    } catch (DavException e) {
      
    }
    JSONObject props = jo.getJSONObject("properties");
    Iterator<String> it = props.keys();
    DavRequest dr = Dav.getDav(ctx.getInitParameter("ServerName")).list(WebClass.davClass);
    dr.setString(WebClass._davClassUri, Money._rdfType);
    String wcMoney = null;
    try {
      wcMoney = dr.execute().getResourceList().get(0).getUri();
    } catch (DavException e) {
      
    }
    VocLoader vocLoader = null;
    try {
      vocLoader = new VocLoader(true);
    } catch (Exception e) {
      
    }

    HashMap<String, String> h = new HashMap();
    while (it.hasNext()) {
      String propName = it.next();
      JSONObject annotations = props.getJSONObject(propName);
      String range = null;
      try {
        range = annotations.getString("range");
      } catch (JSONException e) {
        
      }
      // Assume that if there is no range then it's a string
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
          if (range != null) {
            pCl = InlineProperty.davClass;
            iRange = wcMoney;
  //          if (currency != null)
  //            p.setString(InlineProperty._currency, currency);
          }
  
        }
        else {
          pCl = getPropertyType(range);
          if (pCl == null) { 
            pCl = ResourceProperty.davClass;
            String d = hostAlias.getDirByHostUrl(ctx.getInitParameter("ServerName"));
            String rUri = hostAlias.getAliasByHost(d) + "/voc/dev/" + range.replace(".", "/");
            
   
            iRange = h.get(rUri);
            if (iRange == null) {
              dr = Dav.getDav(ctx.getInitParameter("ServerName")).list(WebClass.davClass);
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
      preq.setString(pCl.getProperty(WebProperty._name).getUri(), propName);
      preq.setString(pCl.getProperty(WebProperty._label).getUri(), propName.replaceAll("_", " "));
      
      if (iRange != null)
        preq.setResourceUri(InlineProperty._range, iRange);
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
    DavResource app = null;
    try {
      app = Dav.getDav().propfind(aUri).execute().getResource();
    } catch (DavException e) {
      
    }
    loadWebClass(app, webClass, vocLoader, req, res);
  }
  
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
    case "long":
      return LongProperty.davClass;
    case "image":
      return ImageProperty.davClass;
    default:
      return null;
    }
  }
  private boolean loadWebClass(DavResource app, DavResource webClass, VocLoader loader, DavServletRequest req, DavServletResponse res) {
    try {
      IncrementalPublishing ip = new IncrementalPublishing(app, webClass, ctx, loader, req, res);
      return ip.publishOne();
    } catch (Exception e) {
      e = e;
    }
    return true;
   }

}