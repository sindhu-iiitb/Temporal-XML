package org.iiitb.dm.temporalxmlwrapper;

import java.text.DateFormat;
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

public class TemporalDelete {
	String xupdate;
	String collectionName;
	String xmlFileName;
	String adminname;
	String password;
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	 Date date = new Date();
	public TemporalDelete(String deleteElement, String collectionName, String xmlFileName,String adminname,String password) {
		this.xupdate = deleteElement;
		this.collectionName = collectionName;
		this.xmlFileName = xmlFileName;
		this.adminname = adminname;
		this.password = password;
	}
	
	public void deleteElementInResourceAndHistroy() throws Exception
	{
		String declarations = "<?xml version=\"1.0\"?>" +
				"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
		String xupdate_curr = declarations + xupdate + "</xu:modifications>";
    	String xupdate_new = declarations;
        AdminService adminService = new AdminServiceLocator();
        Admin admin = adminService.getAdmin();
        QueryService queryService = new QueryServiceLocator();
        Query query = queryService.getQuery();
        System.out.println(dateFormat.format(date));
		String session = admin.connect(adminname, password);
		
    	int index1 = xupdate.indexOf("select");
    	String xupdate_concat = xupdate.substring(index1);
    	String pathsegments[] = xupdate_concat.split("\"");
    	xupdate_concat = pathsegments[0]+ " \"" + pathsegments[1];
//    	System.out.println(xupdate_concat);
    	xupdate_new = xupdate_new + "<xu:update " + xupdate_concat + "[@tend='9999-12-31']/child::node()/@tend\">" + dateFormat.format(date) + "</xu:update></xu:modifications>";
    	String xupdate_mainele = xupdate_new.replace("/child::node()", "");
		
////------Ancestors update------
			String ancestorsupdate;	
			int index2 = xupdate.indexOf("<xu:remove");
			String xupdate_str = xupdate.substring(index2, xupdate.length());
			int index3 = xupdate_str.indexOf('>');
			System.out.println(index3);
			String xupdate_del = xupdate_str.substring(0, index3+1);
			System.out.println(xupdate_del +"what is this");
			String append = "/ancestor::*/@tend\">"+ dateFormat.format(date) + "</xu:update>";
			int index4 = xupdate_del.lastIndexOf('\"');
			System.out.println(index4);
			xupdate_del = xupdate_del.substring(0, index4);
			xupdate_del = xupdate_del +append;
//			xupdate_del = xupdate_del.replace("\">", append);
			String pathsegements[] = xupdate_del.split("select=");
			ancestorsupdate =  declarations;	
			ancestorsupdate = ancestorsupdate + "<xu:update select=" + pathsegements[1] + "</xu:modifications>";		
		
		
			System.out.println(xupdate_new);
			System.out.println(xupdate_curr);
			System.out.println(ancestorsupdate);
			System.out.println(xupdate_mainele);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml", ancestorsupdate);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml", xupdate_new);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" +"H" + xmlFileName + ".xml", xupdate_mainele);
		admin.xupdateResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName + ".xml", xupdate_curr);
		String data = query.getResource(session, XmldbURI.ROOT_COLLECTION + "/" + collectionName + "/" + xmlFileName +".xml",true, true);
		System.out.println(data);
		admin.disconnect(session);
	}
}
