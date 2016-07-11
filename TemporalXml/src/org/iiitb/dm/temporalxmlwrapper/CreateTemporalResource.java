/**
	 * Creates history file along with the original xml
	 * @param collection name ("/db/employees") 
	 * @param file path 	  ("/home/iiitb/testemp.xml")
	 * @author Sirisha Ch
	 * 
	 * 
	 */



package org.iiitb.dm.temporalxmlwrapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.exist.xmldb.XmldbURI;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;


public class CreateTemporalResource {

	String collection;
	String file;
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	Date date = new Date();
	Date enddate = null;
	public CreateTemporalResource(String collection, String file) {
		this.collection = collection;
		this.file = file;
	}
	
	public void createResourceAndHistory() throws Exception
	{
		
		final String URI = "xmldb:exist://localhost:8080/exist/xmlrpc";
		 try
			{
				enddate = dateFormat.parse("9999-12-31");
			}catch(ParseException e)
				{
					e.printStackTrace();
				}
		 
		// initialize driver
			String driver = "org.exist.xmldb.DatabaseImpl";
			Class<?> cl = Class.forName(driver);			
			Database database = (Database)cl.newInstance();
			database.setProperty("create-database", "true");
			DatabaseManager.registerDatabase(database);
		
	  // try to get collection
			Collection col = 
				DatabaseManager.getCollection(URI + collection);
			if(col == null) {
		       Collection root = DatabaseManager.getCollection(URI + XmldbURI.ROOT_COLLECTION);
		       CollectionManagementService mgtService = 
		           (CollectionManagementService)root.getService("CollectionManagementService", "1.0");
		       col = mgtService.createCollection(collection.substring((XmldbURI.ROOT_COLLECTION + "/").length()));
		   }
			File f = new File(file);
			File hf = createHFile(file);
			BufferedReader br = new BufferedReader(new FileReader(hf.getName()));
			 String line = null;
//			 System.out.println("afterrrrrrrrrrs" + hf.getName());
//			 while ((line = br.readLine()) != null) {
//			   System.out.println(line);
//			 }
//		    create new XMLResource
			XMLResource document1 = (XMLResource)col.createResource(f.getName(), "XMLResource");
			document1.setContent(f);
			System.out.print("storing document " + document1.getId() + "...");
			col.storeResource(document1);
			XMLResource document2 = (XMLResource)col.createResource(hf.getName(), "XMLResource");
			document2.setContent(hf);
			System.out.print("storing history document " + document2.getId() + "...");
			col.storeResource(document2);
			System.out.println("ok.");		
			hf.delete();
	}
	
	public File createHFile(String file1)
	{
			String filepathsegments[] = file1.split("/");
			String historyfile = "H"+ filepathsegments[filepathsegments.length-1];
			FileInputStream in = null;
			FileOutputStream out = null;
			File result = null;
			try{
					
					in = new FileInputStream(file1);
				    out = new FileOutputStream(historyfile);
				    result = new File(historyfile);
				    boolean b=result.createNewFile();
				    System.out.println(b);
				    BufferedReader br = new BufferedReader(new InputStreamReader(in));
				    BufferedWriter bw = new BufferedWriter(new FileWriter(historyfile));
				    String currentLine;
				    while ((currentLine = br.readLine()) != null) {
						
				    	if(currentLine.contains(">") && !currentLine.contains("<?xml")){
					    	String pathsegments[] = currentLine.split(">");
					    	if(!pathsegments[0].contains("</")){
					    	pathsegments[0] = pathsegments[0] + " tstart=\"" + dateFormat.format(date)  + "\" " +
					    										"tend=\"" + dateFormat.format(enddate)  + "\"";
					    	}
					    	String newLine = pathsegments[0] + ">";
					    	for (int i = 1; i < pathsegments.length; i++) {
								newLine = newLine + pathsegments[i] + ">";
							}
					    	bw.write(newLine);
							bw.newLine();
				    	}
				    	
				    
				    }
				    bw.close();
			  } catch (FileNotFoundException e) {
					
					e.printStackTrace();
				} catch (IOException e) {
				
					e.printStackTrace();
				}finally {
			         if (in != null) {
			            try {
							in.close();
						} catch (IOException e) {
							
							e.printStackTrace();
						}
			         }
			         if (out != null) {
			            try {
							out.close();
						} catch (IOException e) {
							
							e.printStackTrace();
						}
			         }
			      }
			return result;
	}


	
}
