package org.iiitb.dm.temporalxmlwrapper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.exist.soap.Admin;
import org.exist.soap.AdminService;
import org.exist.soap.AdminServiceLocator;
import org.exist.soap.Query;
import org.exist.soap.QueryService;
import org.exist.soap.QueryServiceLocator;
import org.exist.xmldb.XmldbURI;
import org.exist.xupdate.XUpdateProcessor;

public class TemporalInsert {

	String xupdate;
    String collectionName;
	String xmlFileName;
	String adminname;
	String password;
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	Date date = new Date();
    Date enddate;
	public TemporalInsert(String insertElement, String collectionName, String xmlFileName,String adminname, String password) {
		this.xupdate = insertElement;
		this.collectionName = collectionName;
		this.xmlFileName = xmlFileName;	
		this.adminname = adminname;
		this.password = password;
	}
	
	public void insertResourceAndHistory() throws Exception
	{
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
	String starttime ="<xu:attribute name=\"tstart\">" + dateFormat.format(date) + "</xu:attribute>" ;
	String endtime = "<xu:attribute name=\"tend\">" + dateFormat.format(enddate) + "</xu:attribute>" ;
    AdminService adminService = new AdminServiceLocator();
    Admin admin = adminService.getAdmin();
    QueryService queryService = new QueryServiceLocator();
    Query query = queryService.getQuery();
    System.out.println(dateFormat.format(date));
	String session = admin.connect(adminname, password);
//	"[@tend='9999-12-31']" 
	int index = xupdate.indexOf('>');
	xupdate = xupdate.substring(0, index-1) + "[@tend='9999-12-31']\">" + xupdate.substring(index+1);
	System.out.println(xupdate + "-------");
	int index1 = xupdate.indexOf("<xu:element name");
	int index2 = xupdate.substring(index1,xupdate.length()).indexOf('>');
	xupdate = xupdate.substring(0, index1+index2+1) + starttime + endtime + xupdate.substring(index1+index2+1);
	String xupdate_copy = xupdate;
	String pathsegments[] = new String[100];
	int count = xupdate_copy.length();
	int i =0;
	while( count > 0)
	{
	int index3 = xupdate_copy.indexOf('<');
	int index4 = xupdate_copy.indexOf('>');
	String str = xupdate_copy.substring(index3+1, index4);
	pathsegments[i++] = str;
	xupdate_copy = xupdate_copy.substring(index4+1);
	count = xupdate_copy.length();
	}
	for (int j = 0; j < pathsegments.length; j++) {
		
		if(pathsegments[j] != null)
		{
			if(!pathsegments[j].contains("xu") && !pathsegments[j].contains("/xu") && !pathsegments[j].contains("/"))
			{
				
				String str1 = "<" +pathsegments[j];
				int index5 = xupdate.indexOf(str1);
				int index6 = xupdate.substring(index5,xupdate.length()).indexOf('>');
				xupdate =  xupdate.substring(0, index5+index6+1) + starttime + endtime + xupdate.substring(index5+index6+1);
			}
		}
		
	}
		xupdate = declarations +xupdate;
	
////------Ancestors update------
		String ancestorsupdate;	
		int index7 = xupdate.indexOf("<xu:insert");
		String xupdate_str = xupdate.substring(index7, xupdate.length());
		int index8 = xupdate_str.indexOf('>');
//		System.out.println(index8);
		String xupdate_concat = xupdate_str.substring(0, index8+1);
		String append = "/ancestor::*/@tend\">"+ dateFormat.format(date) + "</xu:update>";
		xupdate_concat = xupdate_concat.replace("\">", append);
		String pathsegements[] = xupdate_concat.split("select=");
		ancestorsupdate =  declarations;	
		ancestorsupdate = ancestorsupdate + "<xu:update select =" + pathsegements[1] + "</xu:modifications>";
		System.out.println(ancestorsupdate);
		System.out.println(xupdate_curr);
		
		xupdate =  xupdate + "</xu:modifications>";
		System.out.println(xupdate);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml",  xupdate);	
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml",  ancestorsupdate);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName + ".xml",  xupdate_curr);
		String data = query.getResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName +  ".xml",true, true);
		System.out.println(data);
		admin.disconnect(session);	    		

	}
}
