package org.iiitb.dm.temporalxmlwrapper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.exist.soap.Admin;
import org.exist.soap.AdminService;
import org.exist.soap.AdminServiceLocator;
import org.exist.soap.Query;
import org.exist.soap.QueryService;
import org.exist.soap.QueryServiceLocator;
import org.exist.xmldb.XmldbURI;
import org.exist.xupdate.XUpdateProcessor;

public class TemporalReplaceByValue {
	String xupdate;
	String collectionName;
	String xmlFileName;
	String adminname;
	String password;
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	Date date = new Date();
	Date enddate;
	public TemporalReplaceByValue(String xReplaceByValue, String  collectionName, String xmlFileName,String adminname, String password ) {
		this.xupdate = xReplaceByValue;
		this.collectionName = collectionName;
		this.xmlFileName = xmlFileName;
		this.adminname = adminname;
		this.password = password;
	}
	public void replaceByValueWrapper() throws Exception
	{

	 	Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);  
	 	
    	 try
    		{
    			enddate = dateFormat.parse("9999-12-31");
    		}catch(ParseException e)
    			{
    				e.printStackTrace();
    			}
    	String declarations = "<?xml version=\"1.0\"?>" +
				"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
    	String xupdate_curr = declarations + xupdate + "</xu:modifications>";
    	String xupdate_copy = xupdate;
//    	[@size='2'][Title='xyz']
    	String ancestorsupdate;	
    	String tinsertelement;
        AdminService adminService = new AdminServiceLocator();
        Admin admin = adminService.getAdmin();
        QueryService queryService = new QueryServiceLocator();
        Query query = queryService.getQuery();
		String session = admin.connect(adminname, password);
//-------ancestors update-----
//Query for updating all the ancestors till node with tend as current date			
		int index1 = xupdate.indexOf("<xu:update select");
		String xupdate_str = xupdate.substring(index1, xupdate.length());
		int index2 = xupdate_str.indexOf('>');
		String xupdate_concat = xupdate_str.substring(0, index2+1);
		String pathsegments[] = xupdate_concat.split("/");
		ancestorsupdate =  declarations;	
		String xupdate_new = xupdate_concat.substring(0, index2-1) + "[@tend='9999-12-31']/ancestor-or-self::*/@tend\">";
		if(xupdate_copy.contains("text"))
		{		ancestorsupdate = ancestorsupdate +xupdate_concat;
				ancestorsupdate = ancestorsupdate.replace("/text()","[@tend='9999-12-31']/ancestor-or-self::*/@tend");
		}
		else
		{
			ancestorsupdate = ancestorsupdate + xupdate_new;

		}
		ancestorsupdate = ancestorsupdate +dateFormat.format(cal.getTime()) + "</xu:update>" +
				"</xu:modifications>";
//		System.out.println(xupdate_concat);
		
//-------InsertElement--------
//Inserts a duplicate element with the given content with tstart as current date and tend as "31-12-9999"		
		int index3 = xupdate_concat.indexOf("\">");
		int index4 = xupdate_str.indexOf("</xu:update>");
		if(xupdate_copy.contains("text"))
		{
		tinsertelement = "<?xml version=\"1.0\"?>" +
				"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">"+
				xupdate_concat.substring(0,index3) + "[@tend='" + dateFormat.format(cal.getTime()) + "']"+ xupdate_concat.substring(index3) +
				"<xu:element name=\"" + pathsegments[pathsegments.length -2]+ "\" >" +
				"<xu:attribute name=\"tstart\">" + dateFormat.format(date) + "</xu:attribute>" +
				"<xu:attribute name=\"tend\">" + dateFormat.format(enddate) + "</xu:attribute>" +
			     xupdate_str.substring(index2+1,index4) +
				"</xu:element>" +
				"</xu:insert-after>"+
				"</xu:modifications>";
		tinsertelement = tinsertelement.replace("xu:update", "xu:insert-after").replace("/text()", "");
		}
		else
		{
//			System.out.println("elseeee");
			tinsertelement = "<?xml version=\"1.0\"?>" +
					"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">"+
					xupdate_concat.substring(0,index3) + "[@tend='" + dateFormat.format(cal.getTime()) + "']"+ xupdate_concat.substring(index3) +
					"<xu:element name=\"" + pathsegments[pathsegments.length -1]+ //"\" >" +
					"<xu:attribute name=\"tstart\">" + dateFormat.format(date) + "</xu:attribute>" +
					"<xu:attribute name=\"tend\">" + dateFormat.format(enddate) + "</xu:attribute>" +
				     xupdate_str.substring(index2+1,index4) +
					"</xu:element>" +
					"</xu:insert-after>"+
					"</xu:modifications>";
			tinsertelement = tinsertelement.replace("xu:update", "xu:insert-after");
		}
		
		System.out.println("insert ele" + " " + tinsertelement);
		System.out.println(ancestorsupdate);
		System.out.println(xupdate_curr);
		
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml",  ancestorsupdate);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml",  tinsertelement);	
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName + ".xml",  xupdate_curr);
		String data = query.getResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName + ".xml",true, true);
		System.out.println(data);
		admin.disconnect(session);
	}

}
