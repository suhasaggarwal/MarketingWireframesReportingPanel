package com.websystique.springmvc.service;



import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jsontocsv.parser.JSONFlattener;
import org.jsontocsv.writer.CSVWriter;

import com.cuberoot.util.AudienceSegmentPopulator;
import com.cuberoot.util.DBConnector;
import com.cuberoot.util.DTOFilter;
import com.cuberoot.util.DTOPopulator;
import com.cuberoot.util.DTOProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.websystique.springmvc.model.CampaignData;
import com.websystique.springmvc.model.Reports;


//Reports are generated corresponding to Audience Section, for user interests,demographics,device properties, geography based reports


public class ReportDAOImpl {

	  public static Map<String,String> citycodeMap;
	  public static Map<String,String> citycodeMap2;
	  public static Map<String,String> countrymap;
	  public static Map<String,List<String>> countrystatemap;
	  public static Map<String,List<String>> countrystatecitymap;
	  public static Map<String,String> citylatlongMap1;
	  
	  public static Map<String,String> campaignIdcampaignNameMap;
 	  
	  
	   
	  static {
	      
	      
	      ReportDAOImpl repDAO = ReportDAOImpl.getInstance();
	      campaignIdcampaignNameMap = repDAO.FetchCampaignIdCampaignNameMap();
	    
	      //    System.out.println(citycodeMap);
	  }
	  
	  
	
	  
	   
	  static {
	      Map<String, String> cityMap = new HashMap<String,String>();
	      String csvFile2 = "/media/raptor/Data/campaigninsights/CodeMaps/citycode.csv";
	      BufferedReader br2 = null;
	      String line2 = "";
	      String cvsSplitBy2 = ",";
	      String key = "";
	      Map<String, String> cityMap1  = new HashMap<String,String>();
	      Map<String, List<String>> cityMap2  = new HashMap<String,List<String>>();
	      List<String> list2 = new ArrayList<String>();
	      
	      try {

	          br2 = new BufferedReader(new FileReader(csvFile2));
	         
	          while ((line2 = br2.readLine()) != null) {

	             try{
	          	// use comma as separator
	            //  line2 = line2.replace(",,",", , ");
	          	//   System.out.println(line);
	          	   String[] geoDetails = line2.split(cvsSplitBy2);
	               key = geoDetails[0];
	               cityMap1.put(key,geoDetails[1]);
	            
	            	  
	              }           
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      citycodeMap = Collections.unmodifiableMap(cityMap1);  
	    
	      //    System.out.println(citycodeMap);
	  }
	  
	    
	  
	  
	  
	  public static Map<String,String> oscodeMap;
	  static {
	      Map<String, String> osMap = new HashMap<String,String>();
	      String csvFile = "/media/raptor/Data/campaigninsights/CodeMaps/os.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> osMap1  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	           //   line = line.replace(",,",", , ");
	          	//   System.out.println(line);
	          	String[] osDetails = line.split(cvsSplitBy);
	              key = osDetails[0];
	              osMap1.put(key,osDetails[1]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      oscodeMap = Collections.unmodifiableMap(osMap1);  
	      System.out.println(oscodeMap);
	  }
	   
	 
	
	  
	  
	  
	  
	  
	  public static Map<String,String> devicecodeMap;
	  static {
	      Map<String, String> deviceMap = new HashMap<String,String>();
	      String csvFile = "/media/raptor/Data/campaigninsights/CodeMaps/deviceType.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> deviceMap1  = new HashMap<String,String>();
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	           //   line = line.replace(",,",", , ");
	          	 //  System.out.println(line);
	          	String[] deviceDetails = line.split(cvsSplitBy);
	              key = deviceDetails[0];
	              deviceMap1.put(key,deviceDetails[1]);
	            //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      devicecodeMap = Collections.unmodifiableMap(deviceMap1);  
	   //   System.out.println(deviceMap);
	  }
	  
	  
	  
	  public static Map<String,String> audienceSegmentMap;
	  public static Map<String,String> audienceSegmentMap1;
	  public static Map<String,String> audienceSegmentMap2;
	  
	  static {
	      Map<String, String> audienceMap = new HashMap<String,String>();
	      String csvFile = "/media/raptor/Data/campaigninsights/CodeMaps/audiencesegment.csv";
	      BufferedReader br = null;
	      String line = "";
	      String cvsSplitBy = ",";
	      String key = "";
	      Map<String, String> audienceMap1  = new HashMap<String,String>();
	      Map<String, String> audienceMap2  = new HashMap<String,String>();
	      
	      try {

	          br = new BufferedReader(new FileReader(csvFile));
	         
	          while ((line = br.readLine()) != null) {

	             try{
	          	// use comma as separator
	             // line = line.replace(",,",", , ");
	          	 //  System.out.println(line);
	          	String[] segmentDetails = line.split(cvsSplitBy);
	              key = segmentDetails[0];
	              audienceMap1.put(key,segmentDetails[1]);
	              audienceMap.put(segmentDetails[1],key);
	              //  hotspotMap1.put(key,hotspotDetails[0]+"@"+hotspotDetails[1]+"@"+hotspotDetails[3]);
	              
	             }
	             catch(Exception e)
	             {
	          	   e.printStackTrace(); 
	               continue;
	             }

	          }


	        
	      
	      }

	      
	      
	      
	catch(Exception e){
		
		e.printStackTrace();
	} 

	      
	      audienceSegmentMap = Collections.unmodifiableMap(audienceMap1);  
	      audienceSegmentMap1 = Collections.unmodifiableMap(audienceMap);
	      
	//   System.out.println(deviceMap);
	  }
	  
	
	  
	  public static Map<String,String> AgeMap;
	  public static Map<String,String> AgeMap1;
	    
	    static {
	        Map<String, String> ageMap = new HashMap<String,String>();
	        String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/agegroup.csv";
	        BufferedReader br5 = null;
	        String line5 = "";
	        String cvsSplitBy5 = ",";
	        String key2 = "";
	        Map<String, String> ageMap1  = new HashMap<String,String>();
	        Map<String, String> ageMap2  = new HashMap<String,String>();
	        
	        try {

	            br5 = new BufferedReader(new FileReader(csvFile5));
	           
	            while ((line5 = br5.readLine()) != null) {

	               try{
	            	// use comma as separator
	          //      line5 = line5.replace(",,",", , ");
	            	 //  System.out.println(line);
	            	String[] ageDetails = line5.split(cvsSplitBy5);
	                key2 = ageDetails[0];
	                ageMap1.put(key2,ageDetails[1]);
	                ageMap2.put(ageDetails[1],key2);
	               }
	               catch(Exception e)
	               {
	            	   e.printStackTrace(); 
	                 continue;
	               }

	            }


	          
	        
	        }

	        
	        
	        
	  catch(Exception e){
	  	
	  	e.printStackTrace();
	  } 

	        
	        AgeMap = Collections.unmodifiableMap(ageMap1);  
	        AgeMap1 = Collections.unmodifiableMap(ageMap2);
	  //   System.out.println(deviceMap);
	    }
	     
	  
	    public static Map<String,String> GenderMap;
	    public static Map<String,String> GenderMap1;
	      
	      static {
	          Map<String, String> genderMap = new HashMap<String,String>();
	          String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/gender.csv";
	          BufferedReader br5 = null;
	          String line5 = "";
	          String cvsSplitBy5 = ",";
	          String key2 = "";
	          Map<String, String> genderMap1  = new HashMap<String,String>();
	          Map<String, String> genderMap2  = new HashMap<String,String>();
	          
	          try {

	              br5 = new BufferedReader(new FileReader(csvFile5));
	             
	              while ((line5 = br5.readLine()) != null) {

	                 try{
	              	// use comma as separator
	           //       line5 = line5.replace(",,",", , ");
	              	 //  System.out.println(line);
	              	String[] genderDetails = line5.split(cvsSplitBy5);
	                  key2 = genderDetails[0];
	                  genderMap1.put(key2,genderDetails[1]);
	                  genderMap2.put(genderDetails[1],key2);
	                 }
	                 catch(Exception e)
	                 {
	              	   e.printStackTrace(); 
	                   continue;
	                 }

	              }


	            
	          
	          }

	          
	          
	          
	    catch(Exception e){
	    	
	    	e.printStackTrace();
	    } 

	          
	          GenderMap = Collections.unmodifiableMap(genderMap1);  
	          GenderMap1 = Collections.unmodifiableMap(genderMap2);
	    //   System.out.println(deviceMap);
	      }
	       
	      public static Map<String,String> BrandMap;
		    public static Map<String,String> BrandMap1;
		      
		      static {
		          Map<String, String> brandMap = new HashMap<String,String>();
		          String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/brand.csv";
		          BufferedReader br5 = null;
		          String line5 = "";
		          String cvsSplitBy5 = ",";
		          String key2 = "";
		          Map<String, String> brandMap1  = new HashMap<String,String>();
		          Map<String, String> brandMap2  = new HashMap<String,String>();
		          
		          try {

		              br5 = new BufferedReader(new FileReader(csvFile5));
		             
		              while ((line5 = br5.readLine()) != null) {

		                 try{
		              	// use comma as separator
		           //       line5 = line5.replace(",,",", , ");
		              	 //  System.out.println(line);
		              	String[] brandDetails = line5.split(cvsSplitBy5);
		                  key2 = brandDetails[0];
		                  brandMap1.put(key2,brandDetails[1]);
		                  brandMap2.put(brandDetails[1],key2);
		                 }
		                 catch(Exception e)
		                 {
		              	   e.printStackTrace(); 
		                   continue;
		                 }

		              }


		            
		          
		          }

		          
		          
		          
		    catch(Exception e){
		    	
		    	e.printStackTrace();
		    } 

		          
		          BrandMap = Collections.unmodifiableMap(brandMap1);  
		          BrandMap1 = Collections.unmodifiableMap(brandMap2);
		    //   System.out.println(deviceMap);
		      }
	      
		      public static Map<String,String> ispMap;
			    public static Map<String,String> ispMap1;
			      
			      static {
			          Map<String, String> ISPMap = new HashMap<String,String>();
			          String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/isp.csv";
			          BufferedReader br5 = null;
			          String line5 = "";
			          String cvsSplitBy5 = ",";
			          String key2 = "";
			          Map<String, String> ISPMap1  = new HashMap<String,String>();
			          Map<String, String> ISPMap2  = new HashMap<String,String>();
			          
			          try {

			              br5 = new BufferedReader(new FileReader(csvFile5));
			             
			              while ((line5 = br5.readLine()) != null) {

			                 try{
			              	// use comma as separator
			           //       line5 = line5.replace(",,",", , ");
			              	 //  System.out.println(line);
			              	String[] ISPDetails = line5.split(cvsSplitBy5);
			                  key2 = ISPDetails[0];
			                  ISPMap1.put(key2,ISPDetails[1]);
			                  ISPMap2.put(ISPDetails[1],key2);
			                 }
			                 catch(Exception e)
			                 {
			              	   e.printStackTrace(); 
			                   continue;
			                 }

			              }


			            
			          
			          }

			          
			          
			          
			    catch(Exception e){
			    	
			    	e.printStackTrace();
			    } 

			          
			          ispMap = Collections.unmodifiableMap(ISPMap1);  
			          ispMap1 = Collections.unmodifiableMap(ISPMap2);
			    //   System.out.println(deviceMap);
			      }
	      
			      public static Map<String,String> screensizeMap;
				    public static Map<String,String> screensizeMapv1;
				      
				      static {
				         // Map<String, String> screensizeMap = new HashMap<String,String>();
				          String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/screensize.csv";
				          BufferedReader br5 = null;
				          String line5 = "";
				          String cvsSplitBy5 = ",";
				          String key2 = "";
				          Map<String, String> screensizeMap1  = new HashMap<String,String>();
				          Map<String, String> screensizeMap2  = new HashMap<String,String>();
				          
				          try {

				              br5 = new BufferedReader(new FileReader(csvFile5));
				             
				              while ((line5 = br5.readLine()) != null) {

				                 try{
				              	// use comma as separator
				           //       line5 = line5.replace(",,",", , ");
				              	 //  System.out.println(line);
				              	String[] screensizeDetails = line5.split(cvsSplitBy5);
				                  key2 = screensizeDetails[0];
				                  screensizeMap1.put(key2,screensizeDetails[1]);
				                  screensizeMap2.put(screensizeDetails[1],key2);
				                 }
				                 catch(Exception e)
				                 {
				              	   e.printStackTrace(); 
				                   continue;
				                 }

				              }


				            
				          
				          }

				          
				          
				          
				    catch(Exception e){
				    	
				    	e.printStackTrace();
				    } 

				          
				          screensizeMap = Collections.unmodifiableMap(screensizeMap1);  
				          screensizeMapv1 = Collections.unmodifiableMap(screensizeMap2);
				    //   System.out.println(deviceMap);
				      }
	      
	      public static Map<String,String> IncomeMap;
	      public static Map<String,String> IncomeMap1;
	        
	        static {
	            Map<String, String> incomeMap = new HashMap<String,String>();
	            String csvFile5 = "/media/raptor/Data/campaigninsights/CodeMaps/incomeMap.csv";
	            BufferedReader br5 = null;
	            String line5 = "";
	            String cvsSplitBy5 = ",";
	            String key2 = "";
	            Map<String, String> incomeMap1  = new HashMap<String,String>();
	            Map<String, String> incomeMap2  = new HashMap<String,String>();
	            
	            try {

	                br5 = new BufferedReader(new FileReader(csvFile5));
	               
	                while ((line5 = br5.readLine()) != null) {

	                   try{
	                	// use comma as separator
	                //    line5 = line5.replace(",,",", , ");
	                	 //  System.out.println(line);
	                	String[] incomeDetails = line5.split(cvsSplitBy5);
	                    key2 = incomeDetails[0];
	                    incomeMap1.put(key2,incomeDetails[1]);
	                    incomeMap2.put(incomeDetails[1],key2);
	                   }
	                   catch(Exception e)
	                   {
	                	   e.printStackTrace(); 
	                     continue;
	                   }

	                }


	              
	            
	            }

	            
	            
	            
	      catch(Exception e){
	      	
	      	e.printStackTrace();
	      } 

	            
	            IncomeMap = Collections.unmodifiableMap(incomeMap1);  
	            IncomeMap1 = Collections.unmodifiableMap(incomeMap2);
	      //   System.out.println(deviceMap);
	        }
	         

	private static ReportDAOImpl INSTANCE;

	private static final Logger logger = Logger.getLogger(ReportDAOImpl.class);

	public static ReportDAOImpl getInstance() {
		
		if(INSTANCE == null)
			return new ReportDAOImpl();
		else
		return INSTANCE;
	}

	public List<Reports> FetchImpressionsData(String startDate, String endDate, String campaignId,String metric, String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		List<Reports> obj2 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 1;
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {
				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select sum(impression)as impression,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date,cuberootcampaign_id";
				}
				else{
				QueryString = "Select sum(impression)as impression,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id,channel";
				}
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

			//	int count = 0;

			//	while (rs.next()) {
			//	    ++count;
				    // Get data from the current row and use it
			//	}
				
			//	System.out.println(count);
				
				
				obj1=DTOPopulator.populateDTO(rs,metric);
                obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
                obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				
				//	jo.put("data",obj1);
				
				//Resultset to json converter
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
             
		finally{
			
			DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return obj3;
	
	
	
	}

	public List<Reports> FetchClicksData(String startDate, String endDate, String campaignId, String metric,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
	    List<Reports> obj3 = null;
		
		
		int ReportCode = 2;
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {

				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select sum(clicks)as clicks,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date,cuberootcampaign_id";
				}
				else{
				QueryString = "Select sum(clicks)as clicks,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
					+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id,channel";
				}
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTO(rs,metric);
				obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				// populate the array
			//	jo.put("data",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
         
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
        	DBUtil.close(connection);
			
		}
		
		
		
		return obj3;
	
	
	
	}

	public List<Reports> FetchConversionsData(String startDate, String endDate, String campaignId, String metric, String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
	    int ReportCode = 3;
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select sum(conversions)as conversions,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date,cuberootcampaign_id";
				}
				else{
				QueryString = "Select sum(conversions)as conversions,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id,channel";
				
				}
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTO(rs,metric);
				obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				//jo.put("data",obj1);
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
         
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		
		return obj3;
	
	
	}



	public List<Reports> FetchCostData(String startDate, String endDate, String campaignId,String metric,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 4;
		
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date,cuberootcampaign_id";
				}
					else{
				QueryString = "Select sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id,channel";
					}
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTO(rs,metric);
				obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				
			//	jo.put("data",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
          
       finally{
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return obj3;
	}

	
	
	public String FetchMetricsDataDatewise(String startDate, String endDate, String campaignId,String channel,String metric,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 4;
		
		String out = "";
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(channel.equals("all") || channel.equals(""))
				{	
				
				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,date FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date";
				}
					else{
				QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,date,cuberootcampaign_id FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id";
					}
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals("")){
						QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,date FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
								+ "'"+startDate + "' AND '" + endDate + "' GROUP by date";
					}
						else{
					QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,date,cuberootcampaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date";
						}
					
					}	
					
					
					
					
				
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTOdatewise(rs,metric);
				//obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				//obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				out = new ObjectMapper().writeValueAsString(obj1);
			//	jo.put("data",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
          
       finally{
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}
	
	
	
	public String FetchDurationData(String startDate, String endDate, String campaignId) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 4;
		
		String out = "";
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				QueryString = "SELECT DATEDIFF(MAX(date),MIN(date)) as duration FROM datawarehouse where cuberootcampaign_id="+campaignId;
		
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTO(rs,null);
				//obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				//obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				out = new ObjectMapper().writeValueAsString(obj1);
			//	jo.put("data",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
          
       finally{
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}
	
	
	public String FetchMetricsData(String startDate, String endDate, String campaignId, String channel,String aggregate,String metric,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 4;
		
		String out = "";
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {


	        	if(channel.equals("all") || channel.equals(""))
				{		
				if(aggregate.equals("true")){	
				
				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate+"'";
				}
					else{
				QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")";
					}
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals("")){
						QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id FROM datawarehouse WHERE date between "
								+ "'"+startDate + "' AND '" + endDate+"' GROUP by cuberootcampaign_id";
					}
						else{
					QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by cuberootcampaign_id,channel";
						}
					
				}
				
				}
				else{
					
					if(aggregate.equals("true")){	
						
						if(campaignId.equals("all") || campaignId.equals("")){
							QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
									+ "'"+startDate + "' AND '" + endDate+"' GROUP by channel";
						}
							else{
						QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
								+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by channel";
							}
						}
						else{
							
							if(campaignId.equals("all") || campaignId.equals("")){
								QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
										+ "'"+startDate + "' AND '" + endDate+"' GROUP by cuberootcampaign_id,channel";
							}
								else{
							QueryString = "Select  sum(impression)as impression,sum(reach)as reach,sum(clicks)as clicks,sum(conversions)as conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by campaign_id,channel";
								}
							
						}
					
				}
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);
				if(aggregate.equals("true")){
				obj1=DTOPopulator.populateDTOMetricWise(rs,metric);
				}
				else{
					
			    obj1 = DTOPopulator.populateDTO(rs, metric);
				}
				
				//obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
				//obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				out = new ObjectMapper().writeValueAsString(obj1);
			//	jo.put("data",obj1);
			
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/metricdata.csv");
				 }
			
			
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
          
       finally{
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}
	
	
	
	
	public String FetchAudienceSegmentImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		List<Reports> obj2 = null;
	//	JSONObject jo = new JSONObject();
		 String out ="";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {
				if(metric.equals("impression") || metric.equals("impressions") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				if(aggregate.equals("true")){
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment";
				else
				QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, cuberootcampaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment";
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,campaign_id audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment";	
					
					
					
				}
				}
				else{
					
					if(aggregate.equals("true")){
					
					   if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment,channel";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment,channel";
					}
					else{
						
						 if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, campaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment,channel";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment,channel";
						
						
						
					}
					
					
				}
				
				}
               	
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				if(aggregate.equals("true")){
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment";
				else
				QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment";
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,campaign_id audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment";	
					
					
					
				}
				}
				else{
					
					if(aggregate.equals("true")){
					
					   if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment,channel";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment,channel";
					}
					else{
						
						 if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost, campaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment,channel";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment,channel";
						
						
						
					}
					
					
				}
				
				}
               
				
				
				if(metric.equals("reach"))
				{
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){
						
						if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment";
						}
						
						else{
							
							if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,campaign_id, audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment";
								else
								QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment FROM datawarehouse WHERE audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment";
							
							
						}
				  }
				   else{
				
					   if(aggregate.equals("true")){
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost, audience_segment FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY audience_segment";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,audience_segment FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
					   				+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,audience_segment";
					   }
					   else{
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,campaign_id, audience_segment FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,audience_segment";
								else
								QueryString ="SELECT SUM(impression)AS impression,SUM(clicks)AS clicks,SUM(reach)AS reach,SUM(conversions)AS conversion,sum(mediacost)as cost,cuberootcampaign_id,campaign_id,audience_segment FROM datawarehouse WHERE channel like '%"+channel+"%' and audience_segment != '' AND date between "
						   				+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,audience_segment"; 
						   
						   
						   
					   }
					   }   
				}
				
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				 if(aggregate.equals("false")){
						
						obj1=DTOPopulator.populateDTOV2(rs,metric);
						
						obj2=AudienceSegmentPopulator.FilterReportDTO(obj1);
						
						out = DTOPopulator.createdNestedDTO(obj2);
						}
					//	jo.put("",obj1);
						else{
					    	
							obj1=DTOPopulator.populateDTOAudienceSegment(rs,metric);
			                obj2=AudienceSegmentPopulator.FilterReportDTO(obj1);
						   
			                out = new ObjectMapper().writeValueAsString(obj2);
						}
				
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/audience_segment.csv");
				 }
				
			//	if(aggregate.equals("false")){
					
				//	out = DTOPopulator.createdNestedDTO(obj2);
			//	}
				//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
          
       finally{
			
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}


	public List<Reports> FetchAudienceSegmentClickData(String startDate, String endDate, String campaignId,String channel) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				QueryString ="SELECT SUM(clicks)AS clicks,date,cuberootcampaign_id,audience_segment,channel FROM datawarehouse WHERE audience_segment != '' AND cuberootcampaign_id in ("+campaignId+") GROUP BY date,cuberootcampaign_id,audience_segment";
				
	            System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				obj1=DTOPopulator.populateDTO(rs,null);

			//	jo.put("data",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
        
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
	     return obj1;
	}


	public String FetchDeviceImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {


				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, device_type FROM datawarehouse WHERE device_type != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, device_type FROM datawarehouse WHERE device_type != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, device_type FROM datawarehouse WHERE device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,device_type";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, device_type FROM datawarehouse WHERE device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,device_type";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,device_type,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, device_type,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, device_type FROM datawarehouse WHERE device_type != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,device_type FROM datawarehouse WHERE device_type != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, device_type,campaign_id,channel FROM datawarehouse WHERE device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,device_type,channel FROM datawarehouse WHERE device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,device_type,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, device_type,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,device_type,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,device_type,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, device_type FROM datawarehouse WHERE device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,device_type FROM datawarehouse WHERE device_type != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, device_type FROM datawarehouse WHERE device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,device_type";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,device_type FROM datawarehouse WHERE device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,device_type";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, device_type FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,device_type FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,device_type";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, device_type,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY device_type,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,device_type FROM datawarehouse WHERE channel like '%"+channel+"%' and device_type != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,device_type";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				 if(aggregate.equals("false")){
						
						obj1=DTOPopulator.populateDTOV2(rs,metric);
						
						out = DTOPopulator.createdNestedDTO(obj1);
						}
					//	jo.put("",obj1);
						else{
					    	
							obj1=DTOPopulator.populateDTO(rs,metric);	
							out = new ObjectMapper().writeValueAsString(obj1);
						}
				
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/device.csv");
				 }
				
				
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

       finally{
			
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		
		return out;

	
	
	}


	public String FetchCityImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		//JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {


				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,city";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,city,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, city,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,campaign_id,channel FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city,channel FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,city";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);
            
				 if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/citydetails.csv");
				 }
				
					
				
				
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}

	
	public String FetchCityOthersImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		//JSONObject jo = new JSONObject();
		List<Reports> obj2 = null;
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {


				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,city";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,city,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, city,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE city != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,campaign_id,channel FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city,channel FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, city,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE city != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, city FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,city";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city FROM datawarehouse WHERE city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,city";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, city,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY city,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,city FROM datawarehouse WHERE channel like '%"+channel+"%' and city != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,city";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				
				

				
				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);

					obj2 = getcityOthers(obj1,metric);
					
				//	out = DTOPopulator.createdNestedDTO(obj2);
				}
			
				else{
			    	
					obj1=DTOPopulator.populateDTO(rs,metric);	
			  
					obj2 = getcityOthers(obj1,metric);
				}
				
				out = new ObjectMapper().writeValueAsString(obj2);
				
				
				//	jo.put("",obj1);
			
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/city.csv");
				 }
			
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}
	
	
	
	
	
	
	public String FetchOSImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, os FROM datawarehouse WHERE os != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, os FROM datawarehouse WHERE os != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, os FROM datawarehouse WHERE os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,os";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, os FROM datawarehouse WHERE os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,os";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,os,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, os,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, os FROM datawarehouse WHERE os != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,os FROM datawarehouse WHERE os != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, os,campaign_id,channel FROM datawarehouse WHERE os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,os,channel FROM datawarehouse WHERE os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,os,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, os,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,os,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,os,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, os FROM datawarehouse WHERE os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,os FROM datawarehouse WHERE os != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, os FROM datawarehouse WHERE os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,os";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,os FROM datawarehouse WHERE os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,os";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, os FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,os FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,os";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, os,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY os,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,os FROM datawarehouse WHERE channel like '%"+channel+"%' and os != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,os";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/os.csv");
				 }
					
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

       finally{
			
    		DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
		
		}
		
		
		
		return out;
	}
	
	
	public List<Reports> FetchReachData(String startDate, String endDate, String campaignId,String channel,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		List<Reports> obj2 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 1;
		
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {
				if(campaignId.equals("all") || campaignId.equals("")){
					QueryString = "Select sum(reach)as reach,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP by date,cuberootcampaign_id";
				}
				else{
				QueryString = "Select sum(reach)as reach,sum(mediacost)as cost,date,cuberootcampaign_id,channel FROM datawarehouse WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+") GROUP by date,cuberootcampaign_id,channel";
				}
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

			//	int count = 0;

			//	while (rs.next()) {
			//	    ++count;
				    // Get data from the current row and use it
			//	}
				
			//	System.out.println(count);
				
				
				obj1=DTOPopulator.populateDTO(rs,null);
                obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
                obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				
				
				//	jo.put("data",obj1);
				
				//Resultset to json converter
			}	

		} catch (Exception e) {
			e.printStackTrace();
		}

       finally{
 			
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return obj3;
	}
	
	
	
	public String FetchCurrencyData(String campaignId) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
		List<Reports> obj2 = null;
	//	JSONObject jo = new JSONObject();
		List<Reports> obj3 = null;
		int ReportCode = 1;
		String out ="";
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();

			if (connection != null) {
				
				QueryString = "Select DISTINCT(advertiser_currency) as currency FROM datawarehouse WHERE cuberootcampaign_id in ("+campaignId+")";
				
				
				System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

			//	int count = 0;

			//	while (rs.next()) {
			//	    ++count;
				    // Get data from the current row and use it
			//	}
				
			//	System.out.println(count);
				
				
				obj1=DTOPopulator.populateDTO(rs,null);
             //   obj2=DTOProcessor.ProcessReportDTO(obj1, startDate, endDate, ReportCode);
             //   obj3=DTOFilter.FilterReportDTO(obj2,startDate, endDate, ReportCode); 	
				out = new ObjectMapper().writeValueAsString(obj1);
				
				//	jo.put("data",obj1);
				
				//Resultset to json converter
			}	

		} catch (Exception e) {
			e.printStackTrace();
		}

       finally{
 			
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		
		return out;
	}
	
	
	
	
	
	
	
	
	
public String FetchAgeImpData(String metric,String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out= "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, age FROM datawarehouse WHERE age != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, age FROM datawarehouse WHERE age != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, age FROM datawarehouse WHERE age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,age";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, age FROM datawarehouse WHERE age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,age";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,age,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, age,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, age FROM datawarehouse WHERE age != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,age FROM datawarehouse WHERE age != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, age,campaign_id,channel FROM datawarehouse WHERE age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,age,channel FROM datawarehouse WHERE age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,age,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, age,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,age,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,age,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, age FROM datawarehouse WHERE age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,age FROM datawarehouse WHERE age != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, age FROM datawarehouse WHERE age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,age";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,age FROM datawarehouse WHERE age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,age";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, age FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,age FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,age";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, age,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY age,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,age FROM datawarehouse WHERE channel like '%"+channel+"%' and age != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,age";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/agegroup.csv");
				 }
					
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		
       finally{
			
    	   
    	    DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	
	
	public String FetchGenderImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, gender FROM datawarehouse WHERE gender != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, gender FROM datawarehouse WHERE gender != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, gender FROM datawarehouse WHERE gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,gender";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, gender FROM datawarehouse WHERE gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,gender";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,gender,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, gender,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, gender FROM datawarehouse WHERE gender != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,gender FROM datawarehouse WHERE gender != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, gender,campaign_id,channel FROM datawarehouse WHERE gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,gender,channel FROM datawarehouse WHERE gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,gender,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, gender,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,gender,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,gender,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, gender FROM datawarehouse WHERE gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,gender FROM datawarehouse WHERE gender != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, gender FROM datawarehouse WHERE gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,gender";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,gender FROM datawarehouse WHERE gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,gender";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, gender FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,gender FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,gender";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, gender,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY gender,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,gender FROM datawarehouse WHERE channel like '%"+channel+"%' and gender != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,gender";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
				    }
					
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/gender.csv");
				 }
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
 
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	public String FetchIncomeImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, income FROM datawarehouse WHERE income != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, income FROM datawarehouse WHERE income != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, income FROM datawarehouse WHERE income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,income";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, income FROM datawarehouse WHERE income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,income";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,income,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, income,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, income FROM datawarehouse WHERE income != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,income FROM datawarehouse WHERE income != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, income,campaign_id,channel FROM datawarehouse WHERE income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,income,channel FROM datawarehouse WHERE income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,income,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, income,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,income,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,income,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, income FROM datawarehouse WHERE income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,income FROM datawarehouse WHERE income != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, income FROM datawarehouse WHERE income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,income";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,income FROM datawarehouse WHERE income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,income";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, income FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,income FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,income";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, income,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY income,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,income FROM datawarehouse WHERE channel like '%"+channel+"%' and income != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,income";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					
					}
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/income.csv");
				 }
					
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
 
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	
	public String FetchBrandImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, brand FROM datawarehouse WHERE brand != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, brand FROM datawarehouse WHERE brand != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, brand FROM datawarehouse WHERE brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,brand";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, brand FROM datawarehouse WHERE brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,brand";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,brand,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, brand,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, brand FROM datawarehouse WHERE brand != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,brand FROM datawarehouse WHERE brand != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, brand,campaign_id,channel FROM datawarehouse WHERE brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,brand,channel FROM datawarehouse WHERE brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,brand,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, brand,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,brand,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,brand,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, brand FROM datawarehouse WHERE brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,brand FROM datawarehouse WHERE brand != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, brand FROM datawarehouse WHERE brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,brand";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,brand FROM datawarehouse WHERE brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,brand";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, brand FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,brand FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,brand";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, brand,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY brand,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,brand FROM datawarehouse WHERE channel like '%"+channel+"%' and brand != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,brand";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				
				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/brand.csv");
				 }
					
			
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
 
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	public String FetchISPImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,serviceprovider";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,serviceprovider";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,serviceprovider,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, serviceprovider,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, serviceprovider,campaign_id,channel FROM datawarehouse WHERE serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,serviceprovider,channel FROM datawarehouse WHERE serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,serviceprovider,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, serviceprovider,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,serviceprovider,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,serviceprovider,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,serviceprovider";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,serviceprovider FROM datawarehouse WHERE serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,serviceprovider";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, serviceprovider FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,serviceprovider FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,serviceprovider";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, serviceprovider,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY serviceprovider,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,serviceprovider FROM datawarehouse WHERE channel like '%"+channel+"%' and serviceprovider != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,serviceprovider";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
					
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/ISP.csv");
				 }
					
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
 
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	public String FetchScreenSizeImpData(String metric, String startDate, String endDate, String campaignId,String channel,String aggregate,String download) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<Reports> obj1 = null;
	//	JSONObject jo = new JSONObject();
		String out = "";
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				if(metric.equals("impressions") || metric.equals("impression") )
				{
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, screensize FROM datawarehouse WHERE screensize != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize";
				else
				QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, screensize FROM datawarehouse WHERE screensize != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize";
				
				}
				else{
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,campaign_id, screensize FROM datawarehouse WHERE screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,screensize";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id,campaign_id, screensize FROM datawarehouse WHERE screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,screensize";
					
					
					
				}
				
				
				}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,channel ";
						else
						QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize,channel";
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion,screensize,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,campaign_id,channel ";
							else
							QueryString ="SELECT SUM(impression)AS impression,SUM(conversions)AS conversion, cuberootcampaign_id, campaign_id, screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id, screensize,channel";
						
					}
					
					
					}	
				}
				
				if(metric.equals("clicks") || metric.equals("click") )
				{
				
				if(channel.equals("all") || channel.equals(""))
				{		
				
				if(aggregate.equals("true")){	
							
				if(campaignId.equals("all") || campaignId.equals(""))
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, screensize FROM datawarehouse WHERE screensize != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize";
				else
				QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,screensize FROM datawarehouse WHERE screensize != '' AND date between "
							+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize";
				
					}
					
					else{
						
						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, screensize,campaign_id,channel FROM datawarehouse WHERE screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,screensize,channel FROM datawarehouse WHERE screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,screensize,channel";
						
						
						
					}
					
					
					
					}
				else{
					
					if(aggregate.equals("true")){	
					
					if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,channel";
						else
						QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize,channel";
					}
					else{
					
			

						if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion, screensize,campaign_id,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,campaign_id,channel";
							else
							QueryString ="SELECT SUM(clicks)AS clicks,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,screensize,channel FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,screensize,channel";
					
					
					
					
					
					
					}
				
				}
               
				}	
				
				if(metric.equals("reach"))
				{
					
					
					
					if(channel.equals("all") || channel.equals(""))
					{
					
						if(aggregate.equals("true")){			
						
					    if(campaignId.equals("all") || campaignId.equals(""))
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, screensize FROM datawarehouse WHERE screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize";
						else
						QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,screensize FROM datawarehouse WHERE screensize != '' AND date between "
									+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize";
					}
					
					else{
						
						    if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,campaign_id, screensize FROM datawarehouse WHERE screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY campaign_id,screensize";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,screensize FROM datawarehouse WHERE screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,screensize";
						
						
					}
					
				  }
				   else{
				
					
					   if(aggregate.equals("true")){	  
					   
					   if(campaignId.equals("all") || campaignId.equals(""))
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, screensize FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize";
							else
							QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,screensize FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
										+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,screensize";
				   }   
				
					   else{
						   
						   
						   if(campaignId.equals("all") || campaignId.equals(""))
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion, screensize,campaign_id FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' GROUP BY screensize,campaign_id";
								else
								QueryString ="SELECT SUM(reach)AS reach,SUM(conversions)AS conversion,cuberootcampaign_id,campaign_id,screensize FROM datawarehouse WHERE channel like '%"+channel+"%' and screensize != '' AND date between "
											+ "'"+startDate + "' AND '" + endDate + "' AND cuberootcampaign_id in ("+campaignId+")  GROUP BY cuberootcampaign_id,campaign_id,screensize";
					   
					   
					   
					   }
				   
				     } 
				   }
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				if(aggregate.equals("false")){
					
					obj1=DTOPopulator.populateDTOV2(rs,metric);
					
					out = DTOPopulator.createdNestedDTO(obj1);
					}
				//	jo.put("",obj1);
					else{
				    	
						obj1=DTOPopulator.populateDTO(rs,metric);	
						out = new ObjectMapper().writeValueAsString(obj1);
					}
				 if(download.equals("true"))
				 {
				 List<Map<String, String>> flatJson = JSONFlattener.parseJson(out);
				// Using the default separator ','
				// If you want to use an other separator like ';' or '\t' use
				// CSVWriter.getCSV(flatJSON, separator) method
				 CSVWriter.writeToFile(CSVWriter.getCSV(flatJson), "/root/screensize.csv");
				 }
					
					
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
 
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		}
		
		
		return out;
	}
	
	
	
	
	

	public List<CampaignData> extractCuberootCampaignIds(String startDate, String endDate) {
		Connection connection = null;
		Statement preparedStatement = null;
		ResultSet rs = null;
		String QueryString = null;
		List<CampaignData> obj1 = new ArrayList<CampaignData>();
	//	JSONObject jo = new JSONObject();
		
		try {
			DBConnector obj = new DBConnector();
			connection = obj.getPooledConnection();


			if (connection != null) {

				QueryString ="SELECT DISTINCT(cuberootcampaign_id)AS campIds FROM datawarehouse  WHERE date between "
						+ "'"+startDate + "' AND '" + endDate + "'";
				
                System.out.println(QueryString+"\n");
				
				preparedStatement = connection.createStatement();
				
				rs = preparedStatement.executeQuery(QueryString);

				 while (rs.next()) {
			     String campId = rs.getString("campIds");
			     CampaignData cd = new CampaignData();
			     cd.setCampaign_id(campId);
			     cd.setCampaign_name("Test Campaign Name");
				 obj1.add(cd);
				 }
				
			//	jo.put("",obj1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
  
        finally{
			
        	DBUtil.close(rs);
			DBUtil.close(preparedStatement);
			DBUtil.close(connection);
			
		} 
		
		
		
		return obj1;
	}
	

public static List<Reports> getcityOthers(List<Reports> report,String metric){
	
	List<Reports> reportdata = new ArrayList<Reports>();
	Integer accumulatedCountConversion = 0;
	Integer accumulatedCountmetric = 0;
	
	for (int i = 0; i < report.size(); i++)
     {
       Reports obj = report.get(i);
       
       
       if(i<10){
       	
       reportdata.add(obj);
       }
       else
       {	   
    	   
    	   
    	 accumulatedCountConversion = accumulatedCountConversion + (int)Double.parseDouble(report.get(i).getConversions().replace(",","")); 
         	        	   
         if(metric.equals("reach"))
       	 accumulatedCountmetric = accumulatedCountmetric + (int)Double.parseDouble(report.get(i).getReach().replace(",","")); 
       	 
         if(metric.equals("clicks") || metric.equals("click") )
         accumulatedCountmetric = accumulatedCountmetric + (int)Double.parseDouble(report.get(i).getClicks().replace(",","")); 
       	 
         if(metric.equals("impressions") || metric.equals("impression") )
         accumulatedCountmetric = accumulatedCountmetric + (int)Double.parseDouble(report.get(i).getImpressions().replace(",","")); 
         
	    	  if(i == (report.size()-1)){
	    		 obj.setCity("Others"); 
	    	     obj.setConversions(accumulatedCountConversion.toString());
	    	     if(metric.equals("reach"))
			     obj.setReach(accumulatedCountmetric.toString());
	    		 
	    	     if(metric.equals("clicks") || metric.equals("click") )	 
	    	     obj.setClicks(accumulatedCountmetric.toString()); 
	    	    	 
	    	    if(metric.equals("impressions") || metric.equals("impression"))	 
	    	     obj.setImpressions(accumulatedCountmetric.toString());
	    	   
	    	     obj.setCitylatlong("");
	    	    
	    	     reportdata.add(obj);
	    	  }
	       }
     
     
     
     
     }

   return reportdata;
	
}

public List<CampaignData> extractCampaignIds(String startDate, String endDate,String campaign_id) {
	Connection connection = null;
	Statement preparedStatement = null;
	ResultSet rs = null;
	String QueryString = null;
	List<CampaignData> obj1 = new ArrayList<CampaignData>();
//	JSONObject jo = new JSONObject();
	
	try {
		DBConnector obj = new DBConnector();
		connection = obj.getPooledConnection();


		if (connection != null) {

			QueryString ="SELECT campaign_id,advertiser FROM datawarehouse WHERE date between "
					+ "'"+startDate + "' AND '" + endDate + "' and cuberootcampaign_id ='"+campaign_id+"' group by campaign_id,advertiser" ;
			
            System.out.println(QueryString+"\n");
			
			preparedStatement = connection.createStatement();
			
			rs = preparedStatement.executeQuery(QueryString);

			 while (rs.next()) {
		     String campId = rs.getString("campaign_id");
		     String advertiser = rs.getString("advertiser");
		     CampaignData cd = new CampaignData();
		     cd.setCampaign_id(campId);
		     cd.setCampaign_name(advertiser);
		     obj1.add(cd);
			 }
			
		//	jo.put("",obj1);
		}

	} catch (Exception e) {
		e.printStackTrace();
	}

    finally{
		
    	DBUtil.close(rs);
		DBUtil.close(preparedStatement);
		DBUtil.close(connection);
		
	} 
	
	
	
	return obj1;
}

public static HashMap<String,String> FetchCampaignIdCampaignNameMap() {
	Connection connection = null;
	Statement preparedStatement = null;
	ResultSet rs = null;
	String QueryString = null;
	List<Reports> obj1 = null;
//	JSONObject jo = new JSONObject();
	String out = "";
	HashMap<String,String> campaignIdCampaignNameMap = new HashMap<String,String>();
	try {
		DBConnector obj = new DBConnector();
		connection = obj.getPooledConnection();


		if (connection != null) {

		
			QueryString ="SELECT campaign_id,advertiser FROM datawarehouse";
			
			
			
            System.out.println(QueryString+"\n");
			
			preparedStatement = connection.createStatement();
			
			rs = preparedStatement.executeQuery(QueryString);

			while (rs.next()) {
			     String campId = rs.getString("campaign_id");
			     String advertiser = rs.getString("advertiser");
			     if(!advertiser.isEmpty())
			     campaignIdCampaignNameMap.put(campId, advertiser); 
			     
				 }
		
		
		
		}

	} catch (Exception e) {
		e.printStackTrace();
	}

   finally{
		
		DBUtil.close(rs);
		DBUtil.close(preparedStatement);
		DBUtil.close(connection);
	
	}
	
	
	
	return campaignIdCampaignNameMap;
}


}
