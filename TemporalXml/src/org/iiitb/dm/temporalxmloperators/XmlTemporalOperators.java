package org.iiitb.dm.temporalxmloperators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import javax.xml.rpc.ServiceException;

import org.exist.soap.Admin;
import org.exist.soap.AdminService;
import org.exist.soap.AdminServiceLocator;
import org.exist.soap.Query;
import org.exist.soap.QueryService;
import org.exist.soap.QueryServiceLocator;
import org.exist.xmldb.EXistResource;
import org.exist.xupdate.XUpdateProcessor;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.CompiledExpression;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceIterator;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.XQueryService;

public class XmlTemporalOperators {
	
	String adminusername;
	String password;
	Collection col;
/**
 * Constructor
 * @param adminusername
 * @param password
 * @param col
 */
	
	
	
 public XmlTemporalOperators(String adminusername,String password,Collection col) {
	this.adminusername=adminusername;
	this.password=password;
	this.col=col;
}
	
 /**
	 * Creates a past history file
	 * @param query --- to get the past file name
	 * @param col --- collection object
	 * @return
	 */
	private Resource createRecords(String query,Collection col){
	
		String[] resourcepath=query.split(Pattern.quote(".xml"));
		String[] retrieve=resourcepath[0].split("/");
		XMLResource res = null ;
		try {
			res= (XMLResource)col.getResource("H"+retrieve[retrieve.length-1]+".xml");
			 File f1=new File(retrieve[retrieve.length-1]+".xml");
			 f1=createLocal(f1, res);
			 //creating history file
			 res=(XMLResource)col.createResource(retrieve[retrieve.length-1]+"_temp.xml", "XMLResource");
			  if(!f1.canRead()) {
	                System.out.println("cannot read file " );
	                return null;
	            }
	            res.setContent(f1);
	            col.storeResource(res);
	            f1.delete();
			
		} catch (XMLDBException e) {
			
			e.printStackTrace();
		}
	
		return res;

	}
	/**
	 * File operations to create the past file with name same as xml file and with _past extension
	 * @param f1
	 * @param res
	 * @return
	 */
	
	private File createLocal(File f1, XMLResource res) {
		BufferedWriter bw;
		
		try {
			String thisLine[]=res.getContent().toString().split("\n");
	
			bw = new BufferedWriter(new FileWriter(f1));
			for (int i = 0; i < thisLine.length; i++) {
			
					bw.write(thisLine[i]);
					bw.flush();
					bw.newLine();
			}
		

		} catch (IOException e) {
		
			e.printStackTrace();
		} catch (XMLDBException e) {
		
			e.printStackTrace();
		}
		return f1;
	}
 
 /**
  * 
  * PAST Operator
  * 
  * 
  */
	
	/**
	 * Method to be called by the application program 
	 * @param string-- the query passed by the user
	 * @param col--collection object 
	 * @param username--- admin credentials (better place them in the constructor of the operators class)
	 * @param pwd
	 * @return Resource object
	 */

	public String pastOperator(String string) {
		  Resource res1=createRecords(string, this.col);
		  String out="";
		  boolean status=updateResourcePast(string,this.col,this.adminusername,this.password);
		  String segments[]=string.split(Pattern.quote(".xml"));
		  String query= segments[0]+"_temp.xml"+segments[1];
		  Resource res = null;
        try { 
        	//if the root element tend is 9999-12-31 no need to query the past data file
            if(!status){
            XQueryService xqs = (XQueryService) this.col.getService("XQueryService", "1.0");
            xqs.setProperty("indent", "yes");
            CompiledExpression compiled = xqs.compile(query);
            ResourceSet result = xqs.execute(compiled);
            ResourceIterator i = result.getIterator();
            if(!i.hasMoreResources())
            {
            	System.out.println("The requested elements have no past data...");
             	System.err.println("Check XQuery");
            }
            while(i.hasMoreResources()) {
                try {
                    res = i.nextResource();
                    out=out+res.getContent()+"\n";
                    System.out.println(res.getContent());
                } finally {
                    //dont forget to cleanup resources
                    try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
                }
            }
//            System.out.println(res1);
           
            }
            col.removeResource(res1);
        } catch (XMLDBException e) {
			
			e.printStackTrace();
		} finally {
            //dont forget to cleanup
            if(col != null) {
                try { col.close(); } catch(XMLDBException xe) {xe.printStackTrace();}
            }
        }
		return out;
	}

/**
 * Method to delete the present elements in the past file created by  createPastRecords method
 * @param string
 * @param col
 * @param username
 * @param pwd
 * @return boolean
 */
	private boolean updateResourcePast(String string,Collection col , String username, String pwd) {
		boolean status=false; 
		AdminService adminService = new AdminServiceLocator();
	     Admin admin;
		try {
			admin = adminService.getAdmin();
			QueryService queryService = new QueryServiceLocator();
		    Query query = queryService.getQuery();
			String session = admin.connect(username,pwd);
			/**
			 * xupdate query  to remove the present elements 
			 */
			
			String xremove="<xu:remove select=\"/*[contains(@tend,'9999-12-31')]\"></xu:remove></xu:modifications>";
			String declarations = "<?xml version=\"1.0\"?>" +
					"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
	        xremove= declarations+xremove;	
	        
	        String data=null;
	        XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
	        xqs.setProperty("indent", "yes");
	        String[] stringsegments=string.split(".xml");
	        String temporalquery=stringsegments[0]+".xml\")/*[contains(@tend,'9999-12-31')]";
	        CompiledExpression compiled = xqs.compile(temporalquery);
	        ResourceSet result = xqs.execute(compiled);
	        ResourceIterator i = result.getIterator(); 
	       
	        if(!(i.nextResource()==null))
	        {
	        	status=true;
	        	System.out.println("The requested elements have no past data...Check XQuery");
	        }
	        else
	        {	        	//getting path to past collection from the query passed 
	        	String[] pathtocollection=stringsegments[0].split(Pattern.quote("(\""));
	        	String[] pathtopast=pathtocollection[1].split(Pattern.quote(".xml"));
	        	String uripath=pathtopast[0]+"_temp.xml";
	        	String[] segments=xremove.split("select=\"");
	        	xremove=segments[0]+"select=\"/*"+segments[1];
//	            System.out.println(xremove);
		        admin.xupdateResource(session,uripath, xremove);	
				data = query.getResource(session, uripath,true, true);
				
				while(true){
				segments=xremove.split(Pattern.quote("select=\""));		    
				xremove=segments[0]+"select=\"/*"+segments[1];
//		        System.out.println(xremove);
		        admin.xupdateResource(session, uripath, xremove);	
				data = query.getResource(session, uripath,true, true);
		        if(!data.contains("9999-12-31")){
//		        	System.out.println(data);
		        	break;
		        }
				
	        }
	        }
			
		} catch (ServiceException e) {
			
			e.printStackTrace();
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (XMLDBException e) {
			
			e.printStackTrace();
		}
		return status;
		
	}
	
	/**
	 * Method to create a temporary file which contains the elements with in the given interval (tstart,tend)
	 * @param string
	 * @param col
	 * @param tstart
	 * @param tend
	 * @param username
	 * @param pwd
	 */
		
		
	private int updateResourceOverlap(String string,Collection col,String tstart,String tend,String username,String pwd){
		boolean status=false; 
		String[] resourcepath=string.split(Pattern.quote(".xml"));
		string=resourcepath[0]+"_temp.xml"+resourcepath[1];
		AdminService adminService = new AdminServiceLocator();
	     Admin admin;
	     int check=0;
		try {
			admin = adminService.getAdmin();
			QueryService queryService = new QueryServiceLocator();
		    Query query = queryService.getQuery();
			String session = admin.connect(username,pwd);
			// xupdate query  to remove the  elements not present in the given time bounds
			
			String xremove="<xu:remove select=\"/*[(xs:date(./@tstart) gt xs:date('"+tend+"') or  xs:date(./@tend) lt xs:date('"+tstart+"'))]\"></xu:remove></xu:modifications>";
			String declarations = "<?xml version=\"1.0\"?>" +
					"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
			 xremove= declarations+xremove;
	        
	        String data=null;
	        XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
	        xqs.setProperty("indent", "yes");
	        String[] stringsegments=string.split(Pattern.quote(")"));
	        
	        String[] pathtocollection=stringsegments[0].split(Pattern.quote("(\""));
	     	String[] pathtopast=pathtocollection[1].split(".xml");
	     	String uripath=pathtopast[0]+".xml";
	    
	     	String[] segments=xremove.split("select=\"");
	     	String datacheckquery="doc(\""+uripath+")/*";
	     	
			String[] datacheckquery1=string.split(".xml");
	        
	        
	        /**
	    	 *  History tstart > given tend
	    	 */
	        String temporalquery=stringsegments[0]+")/*[xs:date(./@tstart) gt xs:date('"+tend+"') ]";
	        CompiledExpression compiled = xqs.compile(temporalquery);
	        ResourceSet result = xqs.execute(compiled);
	        ResourceIterator i = result.getIterator(); 

	        if(!(i.nextResource()==null))
	        {
	        	System.out.println("The requested elements have no relevant overlapped data with the given time...Check XQuery");
	        	return 0;
	        }
	        
	        /**
	         *  History tend < given tstart query the current file
	         */
	        
	        
	        temporalquery=stringsegments[0]+")/*[xs:date(./@tend) lt xs:date('"+tstart+"') ]";
	      
	        compiled=xqs.compile(temporalquery);
	        result=xqs.execute(compiled);
	        i=result.getIterator();
	        if(!(i.nextResource()==null))
	        {
	        	// query the current file
	        	return 1;
	        }
	        
	        
	        /**
	         *  History tstart >= given tstart  && history tstart <= given tend remove the elements those tstart > given tend
	         */
	        
	        
	        temporalquery=stringsegments[0]+")/*[(xs:date(./@tstart) ge xs:date('"+tstart+"')) and (xs:date(./@tstart) le xs:date('"+tend+"'))]";
	        compiled=xqs.compile(temporalquery);
	        result=xqs.execute(compiled);
	        i=result.getIterator();
	        if(!(i.nextResource()==null))
	        {
	        	
	        
	        	 xremove=declarations+"<xu:remove select=\"/*[xs:date(./@tstart) gt xs:date('"+tend+"') ]\"></xu:remove></xu:modifications>";	
	 			data = query.getResource(session, uripath,true, true);
	 			
	 			 datacheckquery=datacheckquery1[0]+".xml\")/*";
	 			while(true){
	 			segments=xremove.split(Pattern.quote("select=\""));		    
	 			xremove=segments[0]+"select=\"/*"+segments[1];
	 	        
	 	        admin.xupdateResource(session, uripath, xremove);	
	 	        datacheckquery=datacheckquery+"/*";
	 	    
	 	       compiled = xqs.compile(datacheckquery);
	 	        result = xqs.execute(compiled);
	 	         i = result.getIterator(); 
	 	         if((i.nextResource()==null))
	 	         {
	 	         	break;
	 	         }
 			
	         }
	        	return 2;
	        }
	        
	        
	        
	        
	        /**
	         *  History tstart < given tstart remove the elements those tstart > given tend and remove the elements whose tend < given tstart
	         */
	        
	        temporalquery=stringsegments[0]+")/*[xs:date(./@tstart) lt xs:date('"+tstart+"') ]";
	        compiled=xqs.compile(temporalquery);
	      
	        result=xqs.execute(compiled);
	        i=result.getIterator();
	        int k=1;
	        if(!(i.nextResource()==null))
	        {
	        	xremove=declarations+"<xu:remove select=\"/*[xs:date(./@tstart) gt xs:date('"+tend+"') ]\"></xu:remove></xu:modifications>";
	        	String xremove1=declarations+"<xu:remove select=\"/*[xs:date(./@tend) lt xs:date('"+tstart+"') ]\"></xu:remove></xu:modifications>";
	 			datacheckquery=datacheckquery1[0]+".xml\")/*";
 			
	 			while(true){
	 				
	 			segments=xremove.split(Pattern.quote("select=\""));		    
	 			xremove=segments[0]+"select=\"/*"+segments[1];
	 			segments=xremove1.split(Pattern.quote("select=\""));		    
	 			xremove1=segments[0]+"select=\"/*"+segments[1];
	 	        admin.xupdateResource(session, uripath, xremove);	
	 	        datacheckquery=datacheckquery+"/*";
	 	        compiled = xqs.compile(datacheckquery);
	 	        result = xqs.execute(compiled);
	 	         i = result.getIterator();
	 	        if((i.nextResource()==null))
		         {
		         	break;
		         }
	 	        while(true){
	 	        	 
	 	        	String datacheck1=datacheckquery+"["+k+"]";
	 	        	CompiledExpression compiled1 = xqs.compile(datacheck1);  
	 	        	ResourceSet result1= xqs.execute(compiled1);
	 	 	        ResourceIterator i1 = result1.getIterator();
	 	        	 if((i1.nextResource()==null)){ 	        		 
	 	        		 break ;
	 	        	 }
	 	        	 else
	 	        	 {
	 	        		 String datacheck2=datacheck1+"/*";
	 	        		
	 	        		String xremovesplit[]=xremove1.split(Pattern.quote("["));
	 	        		String xremove2=xremovesplit[0]+"["+k+"]["+xremovesplit[1];
	 	        		
	 	        		CompiledExpression compiled2 = xqs.compile(datacheck2);
	 	 	        	 	 	           
	 	 	        	ResourceSet result2= xqs.execute(compiled2);
	 	 	 	        ResourceIterator i2 = result2.getIterator();
	 	 	 	        if(i2.nextResource()==null){
	 	 	 	        	
	 	 	 	        	int kh=	admin.xupdateResource(session, uripath, xremove2);
    						if(kh==1){
    							check=1;
    						}
	 	        		}
	 	 	       
	 	 	 	     
	 	        	 }
	 	        	if(check!=1)
    	        	 
    	        		 
    	        	     k++;
    	        	 
    	        	 check=0;
	 	         }
	 	
	 	         k=1;
	 	       
 			
	         }
	 			return 2;
	        }
	        
	        
	        /**
	         *  History tstart = given tend remove the elements those tstart > given tend 
	         */
	        
	        k=1;
	        temporalquery=stringsegments[0]+")/*[xs:date(./@tend) = xs:date('"+tstart+"') ]";
	        compiled=xqs.compile(temporalquery);
	        result=xqs.execute(compiled);
	        i=result.getIterator();
	        if(!(i.nextResource()==null))
	        {
	        	xremove=declarations+"<xu:remove select=\"/*[xs:date(./@tstart) gt xs:date('"+tend+"') ]\"></xu:remove></xu:modifications>";	
				data = query.getResource(session, uripath,true, true);				
				datacheckquery=datacheckquery1[0]+".xml\")/*";
				while(true){
				segments=xremove.split(Pattern.quote("select=\""));		    
				xremove=segments[0]+"select=\"/*"+segments[1];
		        admin.xupdateResource(session, uripath, xremove);	
		        datacheckquery=datacheckquery+"/*";
		        compiled = xqs.compile(datacheckquery);
		        result = xqs.execute(compiled);
		         i = result.getIterator(); 
		         if((i.nextResource()==null))
		         {
		         	break;
		         }				
	        }
	        	return 2;
	        }
	        
	        
	        
	        /**
	         *  History tend = given tstart  remove the elements whose tend < given tstart
	         */
	        
	        
	        temporalquery=stringsegments[0]+")/*[xs:date(./@tend) eq xs:date('"+tstart+"') ]";
	        compiled=xqs.compile(temporalquery);
	        result=xqs.execute(compiled);
	        i=result.getIterator();
	        if(!(i.nextResource()==null))
	        {

	        	String xremove1=declarations+"<xu:remove select=\"/*[xs:date(./@tend) lt xs:date('"+tstart+"') ]\"></xu:remove></xu:modifications>";	 			
	 			datacheckquery=datacheckquery1[0]+".xml\")/*";
	 			while(true){	 				
	 				segments=xremove1.split(Pattern.quote("select=\""));		    
	 	 			xremove1=segments[0]+"select=\"/*"+segments[1];
	 	 	        datacheckquery=datacheckquery+"/*";
	 	 	       compiled = xqs.compile(datacheckquery);
	 	 	        result = xqs.execute(compiled);
	 	 	         i = result.getIterator();
	 	 	         while(true){
	 	 	        	 
	 	 	        	String datacheck1=datacheckquery+"["+k+"]";
	 	 	        
	 	 	        	CompiledExpression compiled1 = xqs.compile(datacheck1);
	 	 	 	        ResourceSet result1= xqs.execute(compiled1);
	 	 	 	        ResourceIterator i1 = result1.getIterator();
	 	 	        	 if((i1.nextResource()==null)){
	 	 	        		 
	 	 	        		 break ;
	 	 	        	 }
	 	 	        	 else
	 	 	        	 {	 	        		 
	 	 	        		
	 		 	        		 String datacheck2=datacheck1+"/*";
	 		 	        		
	 		 	        		String xremovesplit[]=xremove1.split(Pattern.quote("["));
	 		 	        		String xremove2=xremovesplit[0]+"["+k+"]["+xremovesplit[1];
	 		 	        		
	 		 	        		CompiledExpression compiled2 = xqs.compile(datacheck2);
	 		 	 	        	 	 	           
	 		 	 	        	ResourceSet result2= xqs.execute(compiled2);
	 		 	 	 	        ResourceIterator i2 = result2.getIterator();
	 		 	 	 	        if(i2.nextResource()==null){
	 		 	 	 	        	
	 		 	 	 	        	int kh=	admin.xupdateResource(session, uripath, xremove2);
	 	    						if(kh==1){
	 	    							check=1;
	 	    						}
	 		 	        		}
	 		 	 	       
	 		 	 	 	     
	 		 	        	 }
	 		 	        	if(check!=1)
	 	    	        	 
	 	    	        		 
	 	    	        	     k++;
	 	    	        	 
	 	    	        	 check=0;
	 	 	         }
	 	 	      
	 	 	         k=1;
	 	 	         if((i.nextResource()==null))
	 	 	         {
	 	 	         	break;
	 	 	         }

	 	         }
	 			return 2;
	        }
	        
	        
		} catch (ServiceException e) {
		
			e.printStackTrace();
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (XMLDBException e) {

			e.printStackTrace();
		}
		

		return 3;

	}
		
	/**
	 * Method to be called by the application program 	
	 * @param col
	 * @param query1
	 * @param tstart
	 * @param tend
	 * @return
	 */
		
	public String overlapOperator(String query1,String tstart,String tend){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date now = new Date();
		
		try {
			
			if(sdf.parse(tstart).after(now) || sdf.parse(tend).after(now)){
				System.err.println("Start date cannot be a future date");
				return null;
			}
			else if(sdf.parse(tstart).after(sdf.parse(tend))){
				System.err.println("Start date should be less than end date");
				return null;
			}
			else{
			String out="";
			String hname="";
			Resource res1=createRecords(query1, this.col);
			int status=updateResourceOverlap(query1, this.col,tstart,tend,this.adminusername,this.password);
			  String segments1[]=query1.split(".xml");
			  String queryfirst= segments1[0]+"_temp.xml"+segments1[1];
			  String segmentssplit[]=segments1[0].split("/");
			for (int i = 0; i < segmentssplit.length-1; i++) {
				hname=hname+segmentssplit[i]+"/";
			}
			
			hname=hname+"H"+segmentssplit[segmentssplit.length-1]+".xml";
			hname=hname+segments1[1];
		  Resource res = null;
		
	  try { 
	      
	      XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
	      xqs.setProperty("indent", "yes");
	      CompiledExpression compiled;
	      ResourceSet result ;
	      ResourceIterator i;
	      if(status==1){
	    	  compiled= xqs.compile(query1);
	          result= xqs.execute(compiled);
	          i= result.getIterator();
	          System.out.println("The current data..");
	          if(!i.hasMoreResources())
	          {
	          	System.out.println("The requested elements have no data in given interval...");
	          	System.err.println("Check XQuery");
	          }
	          while(i.hasMoreResources()) {
	              try {
	                  res = i.nextResource();
	                  out=out+res.getContent()+"\n";
	                  System.out.println(res.getContent());
	              } finally {
	                  //dont forget to cleanup resources
	                  try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
	              }
	          }
	      }
	      else if(status==2){
//	    	  System.out.println("query"+queryfirst);
	    	  compiled= xqs.compile(queryfirst);
	          result= xqs.execute(compiled);
	          i= result.getIterator(); 
	          if(!i.hasMoreResources())
	          {
	          	System.out.println("The requested elements have no data in given interval...");
	          	System.err.println("Check XQuery");
	          }
	          while(i.hasMoreResources()) {
	              try {
	                  res = i.nextResource();
	                  out=out+res.getContent()+"\n";
	                  System.out.println(res.getContent());
	              } finally {
	                  //dont forget to cleanup resources
	                  try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
	              }
	          }
	      }
	      else if(status==0 || status==3)
	      {
	    	  System.out.println("The requested elements have no data in given interval...");
	        	System.err.println("Check XQuery");
	      }
	       
	    
	      col.removeResource(res1);
	  } catch (XMLDBException e) {
		
			e.printStackTrace();
		} finally {
	      //dont forget to cleanup
	      if(col != null) {
	          try { col.close(); } catch(XMLDBException xe) {xe.printStackTrace();}
	      }
	  }
		return out;
		}
		
		
//		else
//		{
//			System.err.println("Start date should be less than end date");
//			return null;
//		}
		} catch (ParseException e1) {
		
			e1.printStackTrace();
		}
		return null;
	}
	
	
	
	/**
	 * 
	 * Until Operator
	 * 
	 */
	
	public String untilOperator(String query,String date)
	{
		String out="";
		try {

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date now = new Date();
			if(sdf.parse(date).after(now)){
				System.err.println("Date cannot be a future date");
				return null;
			}
			Resource res1=createRecords(query, this.col);
			if(!updateResourceUntil(query, this.col,date,this.adminusername,this.password)){
			  String segments1[]=query.split(Pattern.quote(".xml"));
			  String queryfirst= segments1[0]+"_temp.xml"+segments1[1];
			  Resource res = null;
		      XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
		      xqs.setProperty("indent", "yes");
		      
		      CompiledExpression compiled = xqs.compile(queryfirst);
		      ResourceSet result = xqs.execute(compiled);
		      ResourceIterator i = result.getIterator(); 
		      if(!i.hasMoreResources())
		      {
		      	System.out.println("The requested elements have no data...");
		      	System.err.println("Check XQuery");
		      }
		      while(i.hasMoreResources()) {
		          try {
		              res = i.nextResource();
		              out=out+res.getContent()+"\n";
		              System.out.println(res.getContent());
		          } finally {
		              //dont forget to cleanup resources
		              try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
		          }
		      }
		      col.removeResource(res1);

			}
		}
			catch (XMLDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally {
		      //dont forget to cleanup
		      if(col != null) {
		          try { col.close(); } catch(XMLDBException xe) {xe.printStackTrace();}
		      }
		  }
		return out;
		
		}
	
	
	/**
	 * 
	 * Update resource until
	 */
	
	
	public boolean updateResourceUntil(String string,Collection col,String date,String username,String pwd){
		boolean status=false; 
		AdminService adminService = new AdminServiceLocator();

	     Admin admin;
		try {
			admin = adminService.getAdmin();
			QueryService queryService = new QueryServiceLocator();
		    Query query = queryService.getQuery();
			String session = admin.connect(username,pwd);
			// xupdate query  to remove the  elements not present in the given time bounds
			String xremove="<xu:remove select=\"/*[(xs:date(./@tstart) gt xs:date('"+date+"'))]\"></xu:remove></xu:modifications>";

			String declarations = "<?xml version=\"1.0\"?>" +
					"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
	        xremove= declarations+xremove;	
	        
	        String data=null;
	        XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
	        xqs.setProperty("indent", "yes");
	        String[] stringsegments=string.split(Pattern.quote(".xml\")"));
	        String temporalquery=stringsegments[0]+"_temp.xml\")/*[(xs:date(./@tstart) gt xs:date('"+date+"') )]";
	  
	        CompiledExpression compiled = xqs.compile(temporalquery);

	        ResourceSet result = xqs.execute(compiled);
	        ResourceIterator i = result.getIterator(); 
	        
	        //System.out.println(i.nextResource());
	        if((i.nextResource()!=null))
	        {
	        	status=true;
	        	System.out.println("The requested elements have no relevant data until the given time...Check XQuery");
	        	return status;
	        }
	        else
	        {
	        	//getting path to past collection from the query passed 
	        	String[] pathtocollection=stringsegments[0].split(Pattern.quote("(\""));
	        	String[] pathtountill=pathtocollection[1].split(".xml");
	        	String uripath=pathtountill[0]+"_temp.xml";
	        	String[] segments=xremove.split("select=\"");
	        	xremove=segments[0]+"select=\"/*"+segments[1];

		        admin.xupdateResource(session,uripath, xremove);	
				data = query.getResource(session, uripath,true, true);
				String datacheckquery=uripath+")/*";
				 String[] datacheckquery1=string.split(".xml");
				 datacheckquery=datacheckquery1[0]+".xml\")/*";
				while(true){
				segments=xremove.split(Pattern.quote("select=\""));		    
				xremove=segments[0]+"select=\"/*"+segments[1];
		        admin.xupdateResource(session, uripath, xremove);	
		        datacheckquery=datacheckquery+"/*";
		       compiled = xqs.compile(datacheckquery);
		        result = xqs.execute(compiled);
		         i = result.getIterator(); 
		         if((i.nextResource()==null))
		         {
		         	break;
		         }		
	        }
	        }
	        
		} catch (ServiceException e) {
		
			e.printStackTrace();
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (XMLDBException e) {

			e.printStackTrace();
		}
		return status;
	}
	
	/**
	 * 
	 * Since Operator
	 * 
	 */
	
	public String sinceOperator(String query,String date)
	{
		String out="";
		try {

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date now = new Date();
			if(sdf.parse(date).after(now)){
				System.err.println("Date cannot be a future date");
				return null;
			}
			Resource res1=createRecords(query, this.col);
			if(!updateResourceSince(query, this.col,date,this.adminusername,this.password)){
			  String segments1[]=query.split(Pattern.quote(".xml"));
			  String queryfirst= segments1[0]+"_temp.xml"+segments1[1];
			  Resource res = null;
		      XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
		      xqs.setProperty("indent", "yes");
		      
		      CompiledExpression compiled = xqs.compile(queryfirst);
		      ResourceSet result = xqs.execute(compiled);
		      ResourceIterator i = result.getIterator(); 
		      if(!i.hasMoreResources())
		      {
		      	System.out.println("The requested elements have no past data...");
		      	System.err.println("Check XQuery");
		      }
		      while(i.hasMoreResources()) {
		          try {
		              res = i.nextResource();
		              out=out+res.getContent()+"\n";
		              System.out.println(res.getContent());
		          } finally {
		              //dont forget to cleanup resources
		              try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
		          }
		      }
		      col.removeResource(res1);

			}
		}
			catch (XMLDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		finally {
		      //dont forget to cleanup
		      if(col != null) {
		          try { col.close(); } catch(XMLDBException xe) {xe.printStackTrace();}
		      }
		  }
		return out;
		
		}
	
	
	/**
	 * 
	 * Update resource until
	 */
	
	
	public boolean updateResourceSince(String string,Collection col,String date,String username,String pwd){
		boolean status=false; 
		
		String[] resourcepath=string.split(Pattern.quote(".xml"));
		string=resourcepath[0]+"_temp.xml"+resourcepath[1];
		AdminService adminService = new AdminServiceLocator();
	     Admin admin;
	     int check=0;
		try {
			admin = adminService.getAdmin();
			QueryService queryService = new QueryServiceLocator();
		    Query query = queryService.getQuery();
			String session = admin.connect(username,pwd);
			// xupdate query  to remove the  elements not present in the given time bounds
			String xremove="<xu:remove select=\"/*[(xs:date(./@tend) lt xs:date('"+date+"'))]\"></xu:remove></xu:modifications>";

			String declarations = "<?xml version=\"1.0\"?>" +
					"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
	        xremove= declarations+xremove;	
	        String[] datacheckquery1=string.split(".xml");
	        String datacheckquery;
	        String segments[];
	        String data=null;
	        XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
	        xqs.setProperty("indent", "yes");
	        String[] stringsegments=string.split(Pattern.quote(")"));
	        String[] pathtocollection=stringsegments[0].split(Pattern.quote("(\""));
	     	String[] pathtopast=pathtocollection[1].split(".xml");
	     	String uripath=pathtopast[0]+".xml";
	  
	        String temporalquery=stringsegments[0]+")/*[(xs:date(./@tend) lt xs:date('"+date+"') )]";
	        CompiledExpression compiled = xqs.compile(temporalquery);
	        System.out.println(temporalquery);
	        ResourceSet result = xqs.execute(compiled);
	        ResourceIterator i = result.getIterator(); 

	      /*  if(!(i.nextResource()==null))
	        {
	        	
	        	status=true;
	        	System.out.println("The requested elements have no relevant data until the given time...Check XQuery");
	        	return status;
	        }
	        else
	        {
*/
	        	temporalquery=stringsegments[0]+")/*[xs:date(./@tstart) gt xs:date('"+date+"') ]";
	            compiled=xqs.compile(temporalquery);
	          
	            result=xqs.execute(compiled);
	            i=result.getIterator();
	            int k=1;
	            if(!(i.nextResource()==null))
	            {
	            	
	            	return false;
	            }

	           
	            	String xremove1=declarations+"<xu:remove select=\"/*[xs:date(./@tend) lt xs:date('"+date+"') ]\"></xu:remove></xu:modifications>";
	     			datacheckquery=datacheckquery1[0]+".xml\")/*";
	    			
	     			while(true){
//	     				System.out.println("in while");

	     			segments=xremove1.split(Pattern.quote("select=\""));		    
	     			xremove1=segments[0]+"select=\"/*"+segments[1];

	
	     	        datacheckquery=datacheckquery+"/*";

	     	        compiled = xqs.compile(datacheckquery);
	     	        result = xqs.execute(compiled);
	     	         i = result.getIterator();
	     	        if((i.nextResource()==null))
	    	         {
	    	         	break;
	    	         }
	     	        while(true){
	     	        	 
	     	        	String datacheck1=datacheckquery+"["+k+"]";
	     	        	CompiledExpression compiled1 = xqs.compile(datacheck1);  
	     	        	ResourceSet result1= xqs.execute(compiled1);
	     	 	        ResourceIterator i1 = result1.getIterator();
	     	        	 if((i1.nextResource()==null)){ 	        		 
	     	        		 break ;
	     	        	 }
	     	        	 else
	     	        	 {
	     	        		 String datacheck2=datacheck1+"/*";
	     	        		
	     	        		String xremovesplit[]=xremove1.split(Pattern.quote("["));
	     	        		String xremove2=xremovesplit[0]+"["+k+"]["+xremovesplit[1];

	     	        		CompiledExpression compiled2 = xqs.compile(datacheck2);
	     	 	        	 	 	           
	     	 	        	ResourceSet result2= xqs.execute(compiled2);
	     	 	 	        ResourceIterator i2 = result2.getIterator();
	     	 	 	        if(i2.nextResource()==null){
	     	 	 	        		
	    						int kh=	admin.xupdateResource(session, uripath, xremove2);
	    						if(kh==1){
	    							check=1;
	    						}
	    							
	     	        		}
	     	 	 	
	     	 	       
	     	        	 }
	     	        	 if(check!=1)
	     	        	 {
	     	        		 
	     	        	     k++;
	     	        	 }
	     	        	 check=0;
	     	         }
	     	
	     	         k=1;    			
	             }
	        
	        
		} catch (ServiceException e) {
		
			e.printStackTrace();
		} catch (RemoteException e) {
			
			e.printStackTrace();
		} catch (XMLDBException e) {

			e.printStackTrace();
		}
		return status;
	}
	
	/**
	 * 
	 * field history operator
	 * 
	 */
	
	  public String getFieldHistory(String query){ 
			String[] resourcepath=query.split(Pattern.quote(".xml"));
			String[] retrieve=resourcepath[0].split("/");
	         String hquery="";
	         String out="";
	         for (int i = 0; i < retrieve.length-1; i++) {
	        	 hquery=hquery+retrieve[i]+"/";
	 		}
	         hquery=hquery+"H"+retrieve[retrieve.length-1]+".xml"+resourcepath[1];
//	         
	         try{
	          XQueryService service = (XQueryService) this.col.getService("XQueryService", "1.0"); 
			  service.setProperty("indent", "yes"); 
			
			  // Execute the query, print the result 
			  ResourceSet result = service.query(hquery); 
			  ResourceIterator i = result.getIterator(); 
			  while (i.hasMoreResources()) { 
			          Resource r = i.nextResource(); 
			          out=out+r.getContent()+"\n";
			          System.out.println((String) r.getContent()); 
			  } 
	         }catch (XMLDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	         return out;
	      }

	 /**
	  * 
	  * On operator 
	  * 
	  */
	
		public String onOperator(String query,String date)
		{
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date now = new Date();
			
			try {
				
				if(sdf.parse(date).after(now)){
					System.err.println("Start date cannot be a future date");
					return null;
				}
				
				String out="";
				String hname="";
				Resource res1=createRecords(query, this.col);
				int status=updateResourceOn(query, this.col,date,this.adminusername,this.password);
				  String segments1[]=query.split(".xml");
				  String queryfirst= segments1[0]+"_temp.xml"+segments1[1];
				  String segmentssplit[]=segments1[0].split("/");
				for (int i = 0; i < segmentssplit.length-1; i++) {
					hname=hname+segmentssplit[i]+"/";
				}
				
				hname=hname+"H"+segmentssplit[segmentssplit.length-1]+".xml";
				hname=hname+segments1[1];
			  Resource res = null;
			
		  try { 
		      
		      XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
		      xqs.setProperty("indent", "yes");
		      CompiledExpression compiled;
		      ResourceSet result ;
		      ResourceIterator i;
		      if(status==1){
		    	  compiled= xqs.compile(query);
		          result= xqs.execute(compiled);
		          i= result.getIterator();
		          System.out.println("The current data..");
		          if(!i.hasMoreResources())
		          {
		          	System.out.println("The requested elements have no data in given interval...");
		          	System.err.println("Check XQuery");
		          }
		          while(i.hasMoreResources()) {
		              try {
		                  res = i.nextResource();
		                  out=out+res.getContent()+"\n";
		                  System.out.println(res.getContent());
		              } finally {
		                  //dont forget to cleanup resources
		                  try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
		              }
		          }
		      }
		      else if(status==2){
//		    	  System.out.println("query"+queryfirst);
		    	  compiled= xqs.compile(queryfirst);
		          result= xqs.execute(compiled);
		          i= result.getIterator(); 
		          if(!i.hasMoreResources())
		          {
		          	System.out.println("The requested elements have no data in given interval...");
		          	System.err.println("Check XQuery");
		          }
		          while(i.hasMoreResources()) {
		              try {
		                  res = i.nextResource();
		                  out=out+res.getContent()+"\n";
		                  System.out.println(res.getContent());
		              } finally {
		                  //dont forget to cleanup resources
		                  try { ((EXistResource)res).freeResources(); } catch(XMLDBException xe) {xe.printStackTrace();}
		              }
		          }
		      }
		      
		       
		    
		      col.removeResource(res1);
		  } catch (XMLDBException e) {
			
				e.printStackTrace();
			} finally {
		      //dont forget to cleanup
		      if(col != null) {
		          try { col.close(); } catch(XMLDBException xe) {xe.printStackTrace();}
		      }
		  }
			return out;
			
			
			
		
			} catch (ParseException e1) {
			
				e1.printStackTrace();
			}
			return null;
			
			}
		
		public int updateResourceOn(String string,Collection col,String date,String username,String pwd){
			int status=0; 
			AdminService adminService = new AdminServiceLocator();
		     Admin admin;
		     String[] resourcepath=string.split(Pattern.quote(".xml"));
				string=resourcepath[0]+"_temp.xml"+resourcepath[1];
			try {
				admin = adminService.getAdmin();
				QueryService queryService = new QueryServiceLocator();
			    Query query = queryService.getQuery();
				String session = admin.connect(username,pwd);			
		        String data=null;
		        int check=0,k=1;
		        XQueryService xqs = (XQueryService) col.getService("XQueryService", "1.0");
		        xqs.setProperty("indent", "yes");
		        String[] stringsegments=string.split(Pattern.quote(")"));
		        
		        /*
		         * when given date < histort tstart =====>No relavant data
		         */
		        String temporalquery=stringsegments[0]+")/*[(xs:date(./@tstart) gt xs:date('"+date+"') )]";
		        CompiledExpression compiled = xqs.compile(temporalquery);
		        ResourceSet result = xqs.execute(compiled);
		        ResourceIterator i = result.getIterator(); 	    
		        if(!(i.nextResource()==null))
		        {
		        	status=0;
		        	System.out.println("The requested elements have no relevant data on the given time...Check XQuery");
		        	return status;
		        }
		        
		        /*
		         * When given date > history tend ======>query current file
		         */

		         temporalquery=stringsegments[0]+")/*[(xs:date(./@tend) lt xs:date('"+date+"') )]";
		         compiled = xqs.compile(temporalquery);
		         result = xqs.execute(compiled);
		         i = result.getIterator(); 
		        if(!(i.nextResource()==null))
		        {
		        	status=1;
		        	return status;
		        }
		        
		        /*
		         * When given date >= history tstart and <=history tend
		        */
		        
		        status=2;
		        String xremove="<xu:remove select=\"/*[(xs:date(./@tstart) gt xs:date('"+date+"'))]\"></xu:remove></xu:modifications>";
				String declarations = "<?xml version=\"1.0\"?>" +
						"<xu:modifications version=\"1.0\" xmlns:xu=\"" + XUpdateProcessor.XUPDATE_NS + "\">";
		        xremove= declarations+xremove;	
		        
		        String[] datacheckquery1=string.split(".xml");
		        String datacheckquery;
		        String segments[];
		        
		       
		       
		        String[] pathtocollection=stringsegments[0].split(Pattern.quote("(\""));
		     	String[] pathtopast=pathtocollection[1].split(".xml");
		     	String uripath=pathtopast[0]+".xml";
		        
		        String xremove1=declarations+"<xu:remove select=\"/*[xs:date(./@tend) lt xs:date('"+date+"') ]\"></xu:remove></xu:modifications>";
	 			datacheckquery=datacheckquery1[0]+".xml\")/*";
				
	 			while(true){
	 				segments=xremove.split(Pattern.quote("select=\""));		    
	 	 			xremove=segments[0]+"select=\"/*"+segments[1];
	 	 			admin.xupdateResource(session, uripath, xremove); 	 			
	 	 			segments=xremove1.split(Pattern.quote("select=\""));	    
	 	 			xremove1=segments[0]+"select=\"/*"+segments[1]; 			
	 	 			datacheckquery=datacheckquery+"/*";

	 	 			compiled = xqs.compile(datacheckquery);
	 	            result = xqs.execute(compiled);
		 	         i = result.getIterator();
		 	        if((i.nextResource()==null))
			         {
			         	break;
			         }
		 	        while(true){
		 	        	 
		 	        	String datacheck1=datacheckquery+"["+k+"]";
		 	        	CompiledExpression compiled1 = xqs.compile(datacheck1);  
		 	        	ResourceSet result1= xqs.execute(compiled1);
		 	 	        ResourceIterator i1 = result1.getIterator();
		 	        	 if((i1.nextResource()==null)){ 	        		 
		 	        		 break ;
		 	        	 }
		 	        	 else
		 	        	 {
		 	        		 String datacheck2=datacheck1+"/*";
		 	        		
		 	        		String xremovesplit[]=xremove1.split(Pattern.quote("["));
		 	        		String xremove2=xremovesplit[0]+"["+k+"]["+xremovesplit[1];
		
		 	        		CompiledExpression compiled2 = xqs.compile(datacheck2);
		 	 	        	 	 	           
		 	 	        	ResourceSet result2= xqs.execute(compiled2);
		 	 	 	        ResourceIterator i2 = result2.getIterator();
		 	 	 	        if(i2.nextResource()==null){
		 	 	 	        		
								int kh=	admin.xupdateResource(session, uripath, xremove2);
								if(kh==1){
									check=1;
								}
									
	 	        		}
	 	 	 	
	 	 	       
	 	        	 }
	 	        	 if(check!=1)
	 	        	 {
	 	        		 
	 	        	     k++;
	 	        	 }
	 	        	 check=0;
	 	         }
	 	
	 	         k=1;    			
	         }	        
			} catch (ServiceException e) {
			
				e.printStackTrace();
			} catch (RemoteException e) {
				
				e.printStackTrace();
			} catch (XMLDBException e) {

				e.printStackTrace();
			}
			return status;
		}
	
}
