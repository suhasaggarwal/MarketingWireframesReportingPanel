package com.cuberoot.util;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.websystique.springmvc.model.Reports;
import com.websystique.springmvc.service.ReportDAOImpl;

//Does computations for CPC,CPM,CPP and embeds in json response

/**
 * Utility for converting ResultSets into DTO
 */
public class DTOPopulator {
    /**
     * Populate a result set into DTO
     
     */
   
	public static NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);
	
	public static List<Reports> populateDTO(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }  
            	  
              if( name.equals("channel"))   {
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
              
                if(obj.getChannel().equals("Facebook")){
            	  
            	  obj.setGoal1("1000002");
            	  obj.setGoal2("423260");
            	  obj.setGoal3("200000");
            	  
                }
             
              
                if(obj.getChannel().equals("Adwords")){
              	  
              	  obj.setGoal1("0");
              	  obj.setGoal2("0");
              	  obj.setGoal3("0");
              	  
                  }
               
                if(obj.getChannel().equals("DBM")){
              	  
              	  obj.setGoal1("0");
              	  obj.setGoal2("0");
              	  obj.setGoal3("0");
              	  
                  }
              
              
               
              
              }
              
              
              if(obj.getChannel() == null){
            	  
                  obj.setGoal1("1000002");
               	  obj.setGoal2("423260");
               	  obj.setGoal3("200000");
                	  
                    }
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              if( name.equals("currency")){
                  obj.setCurrency(resultSet.getObject(i + 1).toString());
                 
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0)
          	    	obj.setCpm(0.0);
            	    else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	}
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0)
            	     obj.setCpc(0.0);
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                	    }
                	 }
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 )
                	obj.setCPConversion(0.0);
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0)
                	obj.setCpp(0.0);
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    }
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double total = 0.0;
           Double maxTotal = 0.0;
           Integer share = 0;
           Integer scaledshare = 0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null)
		      		  total=total+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  total=total+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		                total=total+Double.parseDouble(report.get(i).getReach());
		      		 
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotal)
		   	    	    {
		   	    	    	maxTotal = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                 
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && total !=0.0)
		      	        	 share = (int) round((Double.parseDouble(report.get(i).getImpressions())/total)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && total!=0.0)
		      	        	 share =(int) round((Double.parseDouble(report.get(i).getClicks())/total)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && total!=0.0)
		      	       	     share =(int) round(( Double.parseDouble(report.get(i).getReach())/total)*100,2);
		      	        	

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && maxTotal!=0.0)
		      	            scaledshare = (int) round((Double.parseDouble(report.get(i).getImpressions())/maxTotal)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && maxTotal!=0.0) 
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getClicks())/maxTotal)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && maxTotal!=0.0)  
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getReach())/maxTotal)*100,2);
		      	        	
		      	            
		      	        	report.get(i).setScaledshare(scaledshare.toString());	      	        	 
		      	            report.get(i).setShare(share.toString());
		      	  
		        }
           
		         
		         for(int i=0;i<report.size();i++){
	        	      
	        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
	      	        	 report.get(i).setImpressions(numberFormat.format(Integer.parseInt(report.get(i).getImpressions())));
	      	        
	      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty())
	      	        	 report.get(i).setClicks(numberFormat.format(Integer.parseInt(report.get(i).getClicks())));
	      	      
	      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
	      	       	     report.get(i).setReach(numberFormat.format(Integer.parseInt(report.get(i).getReach())));
	      	        	
	      	           if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
	      	       	     report.get(i).setConversions(numberFormat.format(Integer.parseInt(report.get(i).getConversions())));
	      	        	
	        	     
	      	  
	        }
		         
		         
		         

		         Collections.sort(report, new Comparator<Reports>() {
		 				
		 				public int compare(Reports o1, Reports o2) {
		 					return Double.parseDouble(o1.getScaledshare()) > Double.parseDouble(o2.getScaledshare()) ? -1 : (Double.parseDouble(o1.getScaledshare()) < Double.parseDouble(o2.getScaledshare())) ? 1 : 0;
		 		        }
		 		    });	
           
           
           return report;
    }
   
   
	
	
	public static List<Reports> populateDTOV2(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(resultSet.getObject(i + 1).toString());	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(resultSet.getObject(i + 1).toString());	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }  
            	  
              if( name.equals("channel"))   
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0)
          	    	obj.setCpm(0.0);
            	    else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	}
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0)
            	     obj.setCpc(0.0);
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                	    }
                	 }
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 )
                	obj.setCPConversion(0.0);
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0)
                	obj.setCpp(0.0);
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    }
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double total = 0.0;
           Double maxTotal = 0.0;
           Integer share = 0;
           Integer scaledshare = 0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null)
		      		  total=total+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  total=total+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		                total=total+Double.parseDouble(report.get(i).getReach());
		      		 
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotal)
		   	    	    {
		   	    	    	maxTotal = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                
           /*
           
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && total !=0.0)
		      	        	 share = (int) round((Double.parseDouble(report.get(i).getImpressions())/total)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && total!=0.0)
		      	        	 share =(int) round((Double.parseDouble(report.get(i).getClicks())/total)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && total!=0.0)
		      	       	     share =(int) round(( Double.parseDouble(report.get(i).getReach())/total)*100,2);
		      	        	

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && maxTotal!=0.0)
		      	            scaledshare = (int) round((Double.parseDouble(report.get(i).getImpressions())/maxTotal)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && maxTotal!=0.0) 
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getClicks())/maxTotal)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && maxTotal!=0.0)  
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getReach())/maxTotal)*100,2);
		      	        	
		      	            
		      	        	report.get(i).setScaledshare(scaledshare.toString());	      	        	 
		      	            report.get(i).setShare(share.toString());
		      	  
		        }
           */
		         
		         for(int i=0;i<report.size();i++){
	        	      
	        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
	      	        	 report.get(i).setImpressions(report.get(i).getImpressions());
	      	        
	      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty())
	      	        	 report.get(i).setClicks(report.get(i).getClicks());
	      	      
	      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
	      	       	     report.get(i).setReach(report.get(i).getReach());
	      	        	
	      	           if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
	      	       	     report.get(i).setConversions(report.get(i).getConversions());
	      	        	
	        	     
	      	  
	        }
		         
		         
		         

		      //  Collections.sort(report, new Comparator<Reports>() {
		 				
		 			//	public int compare(Reports o1, Reports o2) {
		 				//	return Double.parseDouble(o1.getScaledshare()) > Double.parseDouble(o2.getScaledshare()) ? -1 : (Double.parseDouble(o1.getScaledshare()) < Double.parseDouble(o2.getScaledshare())) ? 1 : 0;
		 		   //     }
		 		 //   });	
           
           
           return report;
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static List<Reports> populateDTOMetricWise(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }  
            	  
               if( name.equals("channel"))   {
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
              
                if(obj.getChannel().equals("Facebook")){
            	  
            	  obj.setGoal1("1000002");
            	  obj.setGoal2("423260");
            	  obj.setGoal3("200000");
            	  
                }
             
              
                if(obj.getChannel().equals("Adwords")){
              	  
              	  obj.setGoal1("0");
              	  obj.setGoal2("0");
              	  obj.setGoal3("0");
              	  
                  }
               
                if(obj.getChannel().equals("DBM")){
              	  
              	  obj.setGoal1("0");
              	  obj.setGoal2("0");
              	  obj.setGoal3("0");
              	  
                  }
              
              
               
              
              }
              
              
              if(obj.getChannel() == null){
            	  
                  obj.setGoal1("1000002");
               	  obj.setGoal2("423260");
               	  obj.setGoal3("200000");
                	  
                    }
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0)
          	    	obj.setCpm(0.0);
            	    else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	}
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0)
            	     obj.setCpc(0.0);
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                	    }
                	 }
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 )
                	obj.setCPConversion(0.0);
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0)
                	obj.setCpp(0.0);
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    }
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double total = 0.0;
           Double totalImpressions= 0.0;
           Double totalClicks = 0.0;
           Double totalReach = 0.0;
           Double totalConversions = 0.0;
           Double maxTotal = 0.0;
           Integer share = 0;
           Integer scaledshare = 0;
           Integer scaledshareImpressions = 0;
           Integer scaledshareClicks = 0;
           Integer scaledshareReach = 0;
           Integer scaledshareConversions = 0;
           Integer shareImpressions = 0;
           Integer shareClicks = 0;
           Integer shareReach = 0;
           Integer shareConversions = 0;
           Double cpm= 0.0;
           Double cpc = 0.0;
           Double cvr = 0.0;
           Double cpp = 0.0;
           Integer scaledsharecpm = 0;
           Integer scaledsharecpc = 0;
           Integer scaledsharecvr = 0;
           Integer scaledsharecpp = 0;
           Integer sharecpm = 0;
           Integer sharecpc = 0;
           Integer sharecvr = 0;
           Integer sharecpp = 0;
           Double cumulativeTotalRatio = 0.0;
           Double cumulativeTotal = 0.0;
           Double maxRatio = 0.0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null)
		      		  totalImpressions=totalImpressions+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  totalClicks=totalClicks+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		              totalReach=totalReach+Double.parseDouble(report.get(i).getReach());
		      		 
		              
		              if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
			          totalConversions=totalConversions+Double.parseDouble(report.get(i).getConversions());
		              
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotal)
		   	    	    {
		   	    	    	maxTotal = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                 
           
                 cumulativeTotal = totalImpressions + totalClicks + totalReach + totalConversions;
           
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && cumulativeTotal !=0.0)
		      	        	 shareImpressions = (int) round((Double.parseDouble(report.get(i).getImpressions())/cumulativeTotal)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && total!=0.0)
		      	        	 shareClicks =(int) round((Double.parseDouble(report.get(i).getClicks())/cumulativeTotal)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && total!=0.0)
		      	       	     shareReach =(int) round(( Double.parseDouble(report.get(i).getReach())/cumulativeTotal)*100,2);
		      	        	
		      	           
		      	         if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty() && total!=0.0)
		      	       	     shareConversions =(int) round(( Double.parseDouble(report.get(i).getConversions())/cumulativeTotal)*100,2);
		      	        	
		      	           

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && totalImpressions !=0.0)
		      	            scaledshareImpressions = (int) round((Double.parseDouble(report.get(i).getImpressions())/totalImpressions)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && totalImpressions !=0.0) 
		      	        	  scaledshareClicks = (int) round((Double.parseDouble(report.get(i).getClicks())/totalImpressions)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && totalImpressions !=0.0)  
		      	        	  scaledshareReach = (int) round((Double.parseDouble(report.get(i).getReach())/totalImpressions)*100,2);
		      	        	
		      	           
		      	            if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty() && totalImpressions !=0.0)  
		      	        	 scaledshareConversions = (int) round((Double.parseDouble(report.get(i).getConversions())/totalImpressions)*100,2);
		      	           
		      	            
		      	        	report.get(i).setScaledshareClicks(scaledshareClicks.toString());       	 
		      	            report.get(i).setScaledshareConversions(scaledshareConversions.toString());
		      	            report.get(i).setScaledshareImpressions(scaledshareImpressions.toString()); 
		                    report.get(i).setScaledshareReach(scaledshareReach.toString());
		       
		                    report.get(i).setShareClicks(shareClicks.toString());       	 
		      	            report.get(i).setShareConversions(shareConversions.toString());
		      	            report.get(i).setShareImpressions(shareImpressions.toString()); 
		                    report.get(i).setShareReach(shareReach.toString());
		         
		         
		         }
           

		         
		           for(int i=0;i<report.size();i++){
				      	  
				      	      if(report.get(i).getCpm()!=null  && report.get(i).getAudience_segment()==null)
				      		  cpm=cpm+report.get(i).getCpm();
				           
				              if(report.get(i).getCpc() != null)
				           	  cpc=cpc+report.get(i).getCpc();
				            
				              if(report.get(i).getCpp()!= null)
				              cpp=cpp+report.get(i).getCpp();
				      	
				              if(report.get(i).getCPConversion()!= null)
					          cvr=cvr+report.get(i).getCPConversion();
				              
				            
				              
		           
		           
		           }
				                 
		           
		                 cumulativeTotalRatio = cpm + cpc + cpp + cvr;
		           
		                 maxRatio = findMax(cpm,cpc,cpp,cvr);
		                 
				         for(int i=0;i<report.size();i++){
				        	      
				        	       if(report.get(i).getCpm()!=null && cumulativeTotalRatio !=0.0)
				      	        	 sharecpm = (int) round((report.get(i).getCpm()/cumulativeTotalRatio)*100,2);
				      	        
				      	           if(report.get(i).getCpc()!=null && cumulativeTotalRatio !=0.0)
				      	        	 sharecpc =(int) round((report.get(i).getCpc()/cumulativeTotalRatio)*100,2);
				      	      
				      	           if(report.get(i).getCpp() != null && cumulativeTotalRatio !=0.0)
				      	       	    sharecpp =(int) round((report.get(i).getCpp()/cumulativeTotalRatio)*100,2);
				      	        	
				      	           
				      	           if(report.get(i).getCPConversion() != null && cumulativeTotalRatio !=0.0)
				      	       	     sharecvr =(int) round((report.get(i).getCPConversion()/cumulativeTotalRatio)*100,2);
				      	        	
				      	           

				      	         if(report.get(i).getCpm()!=null && cumulativeTotalRatio !=0.0)
				      	        	 scaledsharecpm = (int) round((report.get(i).getCpm()/maxRatio)*100,2);
				      	        
				      	           if(report.get(i).getCpc()!=null && cumulativeTotalRatio !=0.0)
				      	        	 scaledsharecpc =(int) round((report.get(i).getCpc()/maxRatio)*100,2);
				      	      
				      	           if(report.get(i).getCpp() != null && cumulativeTotalRatio !=0.0)
				      	       	    scaledsharecpp =(int) round((report.get(i).getCpp()/maxRatio)*100,2);
				      	        	
				      	           
				      	           if(report.get(i).getCPConversion() != null && cumulativeTotalRatio !=0.0)
				      	       	     scaledsharecvr =(int) round((report.get(i).getCPConversion()/maxRatio)*100,2);
				      	           
				      	            
				      	        	report.get(i).setScaledsharecpm(scaledsharecpm.toString());
				      	            report.get(i).setScaledsharecpc(scaledsharecpc.toString());    
				      	            report.get(i).setScaledsharecpp(scaledsharecpp.toString()); 
				      	            report.get(i).setScaledsharecvr(scaledsharecvr.toString()); 
				      	            report.get(i).setSharecpm(sharecpm.toString());
				      	            report.get(i).setSharecpc(sharecpc.toString());    
				      	            report.get(i).setSharecpp(sharecpp.toString()); 
				      	            report.get(i).setSharecvr(sharecvr.toString()); 
				      	  
				      
				         
				         
				         
				         }
		           
		         
		        
		         
		         
				         
				         for(int i=0;i<report.size();i++){
			        	      
			        	  //     if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
			      	       // 	 report.get(i).setImpressions(numberFormat.format(Integer.parseInt(report.get(i).getImpressions())));
			      	        
			      	       //    if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty())
			      	       // 	 report.get(i).setClicks(numberFormat.format(Integer.parseInt(report.get(i).getClicks())));
			      	      
			      	     //      if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
			      	      // 	     report.get(i).setReach(numberFormat.format(Integer.parseInt(report.get(i).getReach())));
			      	        	
			      	       //    if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
			      	       	//     report.get(i).setConversions(numberFormat.format(Integer.parseInt(report.get(i).getConversions())));
			      	        	
			        	     
			      	  
			        }
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         Collections.sort(report, new Comparator<Reports>() {
		 				
		 				public int compare(Reports o1, Reports o2) {
		 					return Double.parseDouble(o1.getScaledshare()) > Double.parseDouble(o2.getScaledshare()) ? -1 : (Double.parseDouble(o1.getScaledshare()) < Double.parseDouble(o2.getScaledshare())) ? 1 : 0;
		 		        }
		 		    });	
           
           
           return report;
    }
    
    public static List<Reports> populateDTOdatewise(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(resultSet.getObject(i + 1).toString());	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(resultSet.getObject(i + 1).toString());	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }
              if( name.equals("channel"))   
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("click")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }

                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
               
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0){
          	    	obj.setCpm(0.01);
            	    if(metric !=null && metric.equals("cpm")){
            	      obj.setMetric(new Double(0.01).toString());
            	    }
            	  
            	    if(metric !=null && metric.equals("CPM")){
              	      obj.setMetric(new Double(0.01).toString());
              	    }
            	    
            	    }else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	 if(metric !=null && metric.equals("cpm")){
            	    		 obj.setMetric(new Double(round(cpm,2)).toString());
                   	    }
            	        
            	    	 if(metric !=null && metric.equals("CPM")){
            	    		 obj.setMetric(new Double(round(cpm,2)).toString());
                   	    }
            	    
            	    
            	        }
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0){
            	     obj.setCpc(0.01);
            	     
            	     if(metric !=null && metric.equals("cpc")){
               	      obj.setMetric(new Double(0.01).toString());
               	    } 
            	     if(metric !=null && metric.equals("CPC")){
                  	      obj.setMetric(new Double(0.01).toString());
                  	    } 
                	
                	}
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                		 if(metric !=null && metric.equals("cpc")){
                      	      obj.setMetric(new Double(round(cpc,2)).toString());
                      	    } 
                	  
                		 if(metric !=null && metric.equals("CPC")){
                     	      obj.setMetric(new Double(round(cpc,2)).toString());
                     	    } 
                	   
                	
                	}
                	 }
                 
                 
                  if(impressions!=null){
                	  
                	  if(clicks == 0.0 || impressions == 0.0){
                 	     obj.setCtr(0.01);
                 	     if(metric !=null && metric.equals("ctr")){
                    	      obj.setMetric(new Double(0.01).toString());
                    	    } 
                     	
                 	    if(metric !=null && metric.equals("CTR")){
                  	      obj.setMetric(new Double(0.01).toString());
                  	    } 
                	  
                	  }
                     	else{
                     	    
                     		double ctr = round((clicks/impressions),2); 
                     		obj.setCtr(round(ctr,2));
                     		 if(metric !=null && metric.equals("ctr")){
                           	      obj.setMetric(new Double(round(ctr,2)).toString());
                           	    } 
                     	
                     		 if(metric !=null && metric.equals("CTR")){
                          	      obj.setMetric(new Double(round(ctr,2)).toString());
                          	    } 
                    	
                     	
                     	}
                	  
                	  
                	  
                  }
                 
                 
                 
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 ){
                	obj.setCPConversion(0.01);
                	  if(metric !=null && metric.equals("cvr")){
                   	      obj.setMetric(new Double(0.01).toString());
                   	    } 
                	
                	  if(metric !=null && metric.equals("CVR")){
                   	      obj.setMetric(new Double(0.01).toString());
                   	    } 
                	
                	}
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		 if(metric != null && metric.equals("cvr")){
                      	      obj.setMetric(new Double(round(cvr,2)).toString());
                      	    } 
                	
                		 if(metric != null && metric.equals("CVR")){
                     	      obj.setMetric(new Double(round(cvr,2)).toString());
                     	    } 
                	
                	
                	}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0){
                	obj.setCpp(0.01);
                	 if(metric!=null && metric.equals("cpp")){
                  	      obj.setMetric(new Double(0.01).toString());
                  	    } 
                	
                	 if(metric!=null && metric.equals("CPP")){
                 	      obj.setMetric(new Double(0.01).toString());
                 	    } 
                	
                	 if(metric!=null && metric.equals("cpr")){
                 	      obj.setMetric(new Double(0.01).toString());
                 	    } 
               	
               	 if(metric!=null && metric.equals("CPR")){
                	      obj.setMetric(new Double(0.01).toString());
                	    } 
                	
                	
                	
                	}
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    
                	    if(metric!=null && metric.equals("cpp")){
                   	      obj.setMetric(new Double(cpp).toString());
                   	    } 
                	    
                	    if(metric!=null && metric.equals("CPP")){
                     	      obj.setMetric(new Double(cpp).toString());
                     	    } 
                	
                	    if(metric!=null && metric.equals("cpr")){
                     	      obj.setMetric(new Double(cpp).toString());
                     	    } 
                  	    
                  	    if(metric!=null && metric.equals("CPR")){
                       	      obj.setMetric(new Double(cpp).toString());
                       	    } 
                  	
                	
                	
                	
                	
                	}
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double total = 0.0;
           Double maxTotal = 0.0;
           Integer share = 0;
           Integer scaledshare = 0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null)
		      		  total=total+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  total=total+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		                total=total+Double.parseDouble(report.get(i).getReach());
		      		 
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotal)
		   	    	    {
		   	    	    	maxTotal = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                 
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && total !=0.0)
		      	        	 share = (int) round((Double.parseDouble(report.get(i).getImpressions())/total)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && total!=0.0)
		      	        	 share =(int) round((Double.parseDouble(report.get(i).getClicks())/total)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && total!=0.0)
		      	       	     share =(int) round(( Double.parseDouble(report.get(i).getReach())/total)*100,2);
		      	        	

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && maxTotal!=0.0)
		      	            scaledshare = (int) round((Double.parseDouble(report.get(i).getImpressions())/maxTotal)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && maxTotal!=0.0) 
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getClicks())/maxTotal)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && maxTotal!=0.0)  
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getReach())/maxTotal)*100,2);
		      	        	
		      	            
		      	        	report.get(i).setScaledshare(scaledshare.toString());	      	        	 
		      	            report.get(i).setShare(share.toString());
		      	  
		        }
           
		         
		         
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()==null || report.get(i).getImpressions().isEmpty())
		      	        	report.remove(i);
		      	        
		      	           if(report.get(i).getClicks()==null || report.get(i).getClicks().isEmpty())
		      	        	 report.remove(i);
		      	      
		      	           if(report.get(i).getReach() == null || report.get(i).getReach().isEmpty())
		      	        	 report.remove(i);
		      	        	
		        	       if(report.get(i).getCost()==null || report.get(i).getCost().isEmpty())
		        	    	   report.remove(i);
		      	        	
		        	       if(report.get(i).getConversions() ==null || report.get(i).getConversions().isEmpty())
		        	    	   report.remove(i);
		        	       
		        	       if(report.get(i).getCpc() ==null)
		        	    	   report.remove(i);
		        	       
		        	       if(report.get(i).getCpm() == null)
		        	    	   report.remove(i);
		        	      
		        	       if(report.get(i).getCpp() == null)
		        	    	   report.remove(i);
		       
		       
		        	      if(report.get(i).getCtr() == null)
		        	    	  report.remove(i);
		         
		         
		        	      if(report.get(i).getCPConversion() == null)
		        	    	  report.remove(i);
		         
		        	      if(report.get(i).getMetric() == null)
		        	    	  report.remove(i);
		        	      
		         }
		         
		         
           
           
           return report;
    }
   
    
    
    
    
    
    
    
    public static String createdNestedDTOV1(List<Reports> report)
            throws Exception {
    
    	String out = "";
    	String cuberootcampId;
    	String campId;
    	
    	    
    	    Map<String, Set<HashMap<String,List<Reports>>>> cuberootcampaignIdMap = new HashMap<String, Set<HashMap<String,List<Reports>>>>();
    	    HashMap<String,List<Reports>> campIdMap = new HashMap<String,List<Reports>>();
    	    for(int i=0; i<report.size(); i++)
    	     {
    	        cuberootcampId = report.get(i).getCuberootcampaignId();
    	        Set<HashMap<String,List<Reports>>> reportDTO = cuberootcampaignIdMap.containsKey(cuberootcampId) ? cuberootcampaignIdMap.get(cuberootcampId) : new HashSet<HashMap<String,List<Reports>>>();
    	            
    	           campId = report.get(i).getCampaign_id();
    	           Reports obj = report.get(i);
    	           obj.setCuberootcampaignId(null);
    	           obj.setCampaign_id(null);
    	           if(campIdMap.get(campId)==null)
    	           {
    	        	   List<Reports>object = new ArrayList<Reports>();
    	        	   object.add(obj);
    	        	   campIdMap.put(campId,object);
    	           }
    	           else{
    	        	  List<Reports>object1=campIdMap.get(campId);
    	        	  object1.add(obj);
    	        	  campIdMap.put(campId,object1); 
    	        	   
    	           }
    	           reportDTO.add(campIdMap);
    	           cuberootcampaignIdMap.put(cuberootcampId, reportDTO);
    	     
    	     }
                   
    	   
	          
    	   
    	
    
    	out = new ObjectMapper().writeValueAsString(cuberootcampaignIdMap);
        return out;
    
    }
    
    
    
    
    public static String createdNestedDTO(List<Reports> report)
            throws Exception {
    
    	String out = "";
    	String cuberootcampId;
    	String campId;
    	String campaignName = "";
    	    
    	    Map<String, Set<Map<String,List<Reports>>>> cuberootcampaignIdMap = new HashMap<String, Set<Map<String,List<Reports>>>>();
    	    HashMap<String,List<Reports>> campIdMap = new HashMap<String,List<Reports>>();
    	    for(int i=0; i<report.size(); i++)
    	     {
    	        cuberootcampId = report.get(i).getCuberootcampaignId();
    	       // Set<HashMap<String,List<Reports>>> reportDTO = cuberootcampaignIdMap.containsKey(cuberootcampId) ? cuberootcampaignIdMap.get(cuberootcampId) : new HashSet<HashMap<String,List<Reports>>>();
    	            
    	           campId = report.get(i).getCampaign_id();
    	           Reports obj = report.get(i);
    	           obj.setCuberootcampaignId(null);
    	           obj.setCampaign_id(null);
    	           if(campIdMap.get(cuberootcampId + ":" +campId)==null)
    	           {
    	        	   List<Reports>object = new ArrayList<Reports>();
    	        	   object.add(obj);
    	        	   campIdMap.put(cuberootcampId + ":" +campId,object);
    	           }
    	           else{
    	        	  List<Reports>object1=campIdMap.get(cuberootcampId + ":"+ campId);
    	        	  object1.add(obj);
    	        	  campIdMap.put(cuberootcampId + ":"+ campId,object1); 
    	        	   
    	           }
    	         //  reportDTO.add(campIdMap);
    	         //  cuberootcampaignIdMap.put(cuberootcampId, reportDTO);
    	     
    	     }
              
    	   
    	  
    	    
    	    for(Map.Entry<String,List<Reports>> entry : campIdMap.entrySet())
   	     {
	         
    	    	
    	      Double total = 0.0;
    	      Double maxTotal = 0.0;
    	      Integer share = 0;
    	      Integer scaledshare = 0;	
    	    	
    	      String cIds = entry.getKey();
	          List<Reports> reportv1 = entry.getValue();
   	          String [] parts = cIds.split(":");
   	          Map<String,List<Reports>> reportDTO = new HashMap<String,List<Reports>>();
   	          //reportDTO.put(parts[1],reportv1);
   	          campaignName = ReportDAOImpl.campaignIdcampaignNameMap.get(parts[1]);
   	          if(campaignName == null)
   	          {
   	        	  campaignName = "";
   	          }
   	          reportDTO.put("campaignName"+":"+campaignName+","+parts[1],reportv1);
   	          
   	          for(int i=0; i<reportv1.size(); i++){
   	        	  
   	        	  if(Double.parseDouble(reportv1.get(i).getMetric()) > maxTotal){
   	        		     	        	
   	        		  maxTotal = Double.parseDouble(reportv1.get(i).getMetric());
   	        	  }
   	        	  
   	        	  total = total+Double.parseDouble(reportv1.get(i).getMetric());
   	        	  
   	          }
   	     
   	          
   	       for(int i=0; i<reportv1.size(); i++){
	        	  
	        	  share =(int) round((Double.parseDouble(reportv1.get(i).getMetric())/total)*100,2);
	        	  
	        	  scaledshare = (int) round((Double.parseDouble(reportv1.get(i).getMetric())/maxTotal)*100,2);
	        	  
	              reportv1.get(i).setShare(share.toString());
	              
	              reportv1.get(i).setScaledshare(scaledshare.toString());
   	       
   	       }
   	          
   	          reportDTO.put("campaignName"+":"+campaignName+","+parts[1],reportv1);
   	          
   	          
   	          
   	          Set<Map<String,List<Reports>>> reportDTOV1 = cuberootcampaignIdMap.containsKey(parts[0]) ? cuberootcampaignIdMap.get(parts[0]) : new HashSet<Map<String,List<Reports>>>();
   	          reportDTOV1.add(reportDTO);
   	          cuberootcampaignIdMap.put(parts[0], reportDTOV1);
   	          
   	    }
    	
    
    	out = new ObjectMapper().writeValueAsString(cuberootcampaignIdMap);
    	//Pattern p = Pattern.compile("(campaignName:)(.*)(,)", Pattern.CASE_INSENSITIVE);
    //	Pattern p = Pattern.compile("(campaignName:)(.*?)(,)", Pattern.CASE_INSENSITIVE);
    //	Matcher m = p.matcher(out);
   //     System.out.println(m.find());
    	String output = "";
    //	while(m.find()){
    //	System.out.println("campaignName"+'"'+":"+'"'+m.group(2)+'"'+","+'"');	
    //	output=m.replaceAll("campaignName"+'"'+":"+'"'+"$2"+'"'+","+'"');
    //	m= p.matcher(output);
    	//}
    	
    	output= out.replaceAll("(campaignName:)(.*?)(,)","campaignName"+'"'+":"+'"'+"$2"+'"'+","+'"');
    	return output;
    
    }
    
    
    
    public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
    
    
    private static double findMax(double... vals) {
    	   double max = Double.NEGATIVE_INFINITY;

    	   for (double d : vals) {
    	      if (d > max) max = d;
    	   }

    	   return max;
    	}
   
    
    public static List<Reports> populateDTOAudienceSegment(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }  
            	  
              if( name.equals("channel"))   
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(numberFormat.format(Integer.parseInt(resultSet.getObject(i + 1).toString())));	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0)
          	    	obj.setCpm(0.0);
            	    else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	}
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0)
            	     obj.setCpc(0.0);
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                	    }
                	 }
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 )
                	obj.setCPConversion(0.0);
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0)
                	obj.setCpp(0.0);
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    }
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double totalimpressions = 0.0;
           Double totalclicks = 0.0;
           Double totalreach = 0.0;
           Double maxTotalimpressions = 0.0;
           Double maxTotalclicks = 0.0;
           Double maxTotalreach = 0.0;
           Integer shareimpressions = 0;
           Integer shareclicks = 0;
           Integer sharereach =0;
           Integer scaledshareimp = 0;
           Integer scaledshareclicks = 0;
           Integer scaledsharereach = 0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
		      		  totalimpressions=totalimpressions+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  totalclicks=totalclicks+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		              totalreach=totalreach+Double.parseDouble(report.get(i).getReach());
		      		 
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty()){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotalimpressions)
		   	    	    {
		   	    	    	maxTotalimpressions = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotalclicks)
			   	    	    {
			   	    	    	maxTotalclicks = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotalreach)
			   	    	    {
			   	    	    	maxTotalreach = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                 
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && totalimpressions !=0.0)
		      	        	 shareimpressions = (int) round((Double.parseDouble(report.get(i).getImpressions())/totalimpressions)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && totalclicks!=0.0)
		      	        	 shareclicks =(int) round((Double.parseDouble(report.get(i).getClicks())/totalclicks)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && totalreach!=0.0)
		      	       	     sharereach =(int) round(( Double.parseDouble(report.get(i).getReach())/totalreach)*100,2);
		      	        	

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && maxTotalimpressions!=0.0)
		      	            scaledshareimp = (int) round((Double.parseDouble(report.get(i).getImpressions())/maxTotalimpressions)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && maxTotalclicks!=0.0) 
		      	        	  scaledshareclicks = (int) round((Double.parseDouble(report.get(i).getClicks())/maxTotalclicks)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && maxTotalreach!=0.0)  
		      	        	  scaledsharereach = (int) round((Double.parseDouble(report.get(i).getReach())/maxTotalreach)*100,2);
		      	      
		      	           
		      	             if(metric!=null && metric.equals("impressions")){
		                      report.get(i).setShare(shareimpressions.toString());	
		                      report.get(i).setScaledshare(scaledshareimp.toString());
		                     	
		                     }
		                    
		                      if(metric!=null && metric.equals("impression")){
		                    	  report.get(i).setShare(shareimpressions.toString());	
			                      report.get(i).setScaledshare(scaledshareimp.toString());
		                         	
		                         	
		                         } 
		      	           
		      
		                     
		      	  
		        
		                      if(metric!=null && metric.equals("reach")){
		                    	  report.get(i).setShare(sharereach.toString());	
			                      report.get(i).setScaledshare(scaledsharereach.toString());
		                          	
		                          	
		                          }
		                      
		                      
		         
		                      if(metric!=null && metric.equals("clicks")){
		                    	  report.get(i).setShare(shareclicks.toString());	
			                      report.get(i).setScaledshare(scaledshareclicks.toString());
		                          	
		                          	
		                          }
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         
		         }
           
		         
		        
		         
		         
		         

		         Collections.sort(report, new Comparator<Reports>() {
		 				
		 				public int compare(Reports o1, Reports o2) {
		 					return Double.parseDouble(o1.getScaledshare()) > Double.parseDouble(o2.getScaledshare()) ? -1 : (Double.parseDouble(o1.getScaledshare()) < Double.parseDouble(o2.getScaledshare())) ? 1 : 0;
		 		        }
		 		    });	
           
           
		       
		         for(int i=0;i<report.size();i++){
	        	      
	        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
	      	        	 report.get(i).setImpressions(numberFormat.format(Integer.parseInt(report.get(i).getImpressions())));
	      	        
	      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty())
	      	        	 report.get(i).setClicks(numberFormat.format(Integer.parseInt(report.get(i).getClicks())));
	      	      
	      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
	      	       	     report.get(i).setReach(numberFormat.format(Integer.parseInt(report.get(i).getReach())));
	      	        	
	      	           if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
	      	       	     report.get(i).setConversions(numberFormat.format(Integer.parseInt(report.get(i).getConversions())));
	      	        	
	        	     
	      	  
	        }
		         
		         
           return report;
    }
   
   
    public static List<Reports> populateDTOV2AudienceSegment(ResultSet resultSet,String metric)
            throws Exception {
       
    	   List<Reports> report = new ArrayList<Reports>();
           Reports reportDTO = null;
    	   String name;
           while (resultSet.next()) {
        	
        	Double impressions = null;
           	Double clicks = null;
           	Double mediacost = null;
           	Double conversions = null;
           	Double reach = null;  
        	String city = null;
        	String citylatlong = null;
        	String [] parts = null;
        	String audiencesegmentcode = null;
        	String gendercode= null;
        	String agegroupcode = null;
        	String devicetypecode = null;
        	String citycode = null;
        	String oscode = null;
        	String brandcode= null;
        	String incomecode= null;
        	String screensizecode = null;
        	String ispcode = null;
        	int total_rows = resultSet.getMetaData().getColumnCount();
            
        	
        	Reports obj = new Reports();
            for (int i = 0; i < total_rows; i++) {
               name = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
              
               if( name.equals("impression")){
                obj.setImpressions(resultSet.getObject(i + 1).toString());
                impressions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                if(metric!=null && metric.equals("impressions")){
                obj.setMetric(resultSet.getObject(i + 1).toString());	
                	
                	
                }
               
                 if(metric!=null && metric.equals("impression")){
                    obj.setMetric(resultSet.getObject(i + 1).toString());	
                    	
                    	
                    }
               
               }
               
               if( name.equals("date"))
            	 obj.setDate(resultSet.getObject(i + 1).toString());
            
               if( name.equals("campaign_id")){
            	  obj.setCampaign_id(resultSet.getObject(i + 1).toString());
            	  obj.setCampaignName(ReportDAOImpl.campaignIdcampaignNameMap.get(resultSet.getObject(i + 1).toString()));
               }  
            	  
              if( name.equals("channel"))   
            	   obj.setChannel(resultSet.getObject(i + 1).toString());
            
            
              if( name.equals("clicks")){
                  obj.setClicks(resultSet.getObject(i + 1).toString());
                  clicks = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("clicks")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              }
                  
                  
              if( name.equals("conversion")){
                  obj.setConversions(resultSet.getObject(i + 1).toString());
                  conversions = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("conversion")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
                  if(metric!=null && metric.equals("conversions")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              }
              
              
              if( name.equals("reach")){
                  obj.setReach(resultSet.getObject(i + 1).toString());              
                  reach = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("reach")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }
              
              
              
              
              if( name.equals("cost")){
                  obj.setCost(resultSet.getObject(i + 1).toString());
                  mediacost = Double.parseDouble(resultSet.getObject(i + 1).toString());
                  if(metric!=null && metric.equals("cost")){
                      obj.setMetric(resultSet.getObject(i + 1).toString());	
                      	
                      	
                      }
              
              
              
              }   
            
              if( name.equals("audience_segment")){
                  obj.setAudience_segment(resultSet.getObject(i + 1).toString());
                  audiencesegmentcode = ReportDAOImpl.audienceSegmentMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAudiencesegmentcode(audiencesegmentcode);
              }
              if( name.equals("city")){
                   parts = resultSet.getObject(i + 1).toString().split(",");
                	  obj.setCity(parts[0]);
                	  citycode = ReportDAOImpl.citycodeMap.get(parts[0]);                	  
                      obj.setCitycode(citycode);
                	  obj.setCitylatlong(parts[1]+","+parts[2]);
              }
              if( name.equals("device_type")){
                  obj.setDevice_type(resultSet.getObject(i + 1).toString());  
                  devicetypecode = ReportDAOImpl.devicecodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setDevicetypecode(devicetypecode);
              }
              if( name.equals("os")){
                  obj.setOs(resultSet.getObject(i + 1).toString());  
                  oscode = ReportDAOImpl.oscodeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setOscode(oscode);
              
              }
              if(name.equals("age")){
                  obj.setAge(resultSet.getObject(i + 1).toString());  
                  agegroupcode = ReportDAOImpl.AgeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setAgecode(agegroupcode);
              }
              if(name.equals("gender")){
                  obj.setGender(resultSet.getObject(i + 1).toString());  
                  gendercode = ReportDAOImpl.GenderMap.get(resultSet.getObject(i + 1).toString());
                  obj.setGendercode(gendercode);
              } 
              
              
              if(name.equals("income")){
                  obj.setIncome(resultSet.getObject(i + 1).toString());  
                  incomecode = ReportDAOImpl.IncomeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIncomecode(incomecode);
              } 
              
              
              if(name.equals("brand")){
                  obj.setBrand(resultSet.getObject(i + 1).toString());  
                  brandcode = ReportDAOImpl.BrandMap.get(resultSet.getObject(i + 1).toString());
                  obj.setBrandcode(brandcode);
              } 
              
              
              if(name.equals("serviceprovider")){
                  obj.setIsp(resultSet.getObject(i + 1).toString());  
                  ispcode = ReportDAOImpl.ispMap.get(resultSet.getObject(i + 1).toString());
                  obj.setIspcode(ispcode);
              } 
              
              if(name.equals("screensize")){
                  obj.setScreensize(resultSet.getObject(i + 1).toString());  
                  screensizecode = ReportDAOImpl.screensizeMap.get(resultSet.getObject(i + 1).toString());
                  obj.setScreensizecode(screensizecode);
              } 
              
              
              
              if(name.equals("duration"))
                  obj.setDuration(resultSet.getObject(i + 1).toString());  
              
              
              
              if(name.equals("cuberootcampaign_id"))
                  obj.setCuberootcampaignId(resultSet.getObject(i + 1).toString());  
              

              if(impressions !=null)
                  {
            	    
            	  if(mediacost!=null){
            	    if(impressions == 0.0 || mediacost == 0.0)
          	    	obj.setCpm(0.0);
            	    else{
            	    	
            	    	double cpm = round(((mediacost/impressions)*1000),2);
            	    	obj.setCpm(round(cpm,2));
            	    	}
            	    	}
            	    	
            	    }
               
                
                if(clicks !=null )
                 {
                	
                	if(mediacost!=null){
                	if(clicks == 0.0 || mediacost == 0.0)
            	     obj.setCpc(0.0);
                	else{
                	    
                		double cpc = round((mediacost/clicks),2); 
                		obj.setCpc(round(cpc,2));
                	    }
                	 }
                 }
             
                if(conversions !=null)
                 {
                	
                	if(mediacost !=null){
                	if(conversions == 0.0 || mediacost == 0.0 )
                	obj.setCPConversion(0.0);
                	else{
                		
                		double cvr = round(((mediacost/conversions)),2);
                		obj.setCPConversion(round(cvr,2));
                		}
                   }
                 }
            
                if(reach !=null)
                {
                	
                	 if(mediacost!=null){
                	if(reach == 0.0 ||  mediacost == 0.0)
                	obj.setCpp(0.0);
                	else{
                	   
                		double cpp = round(((mediacost/reach)*1000),2);
                		cpp = round(cpp,2);
                	    obj.setCpp(cpp);
                	    }
                	 }
                }
            
            
            
            
            }
            report.add(obj);
        
        }
        
           
           Double total = 0.0;
           Double maxTotal = 0.0;
           Integer share = 0;
           Integer scaledshare = 0;
           
           for(int i=0;i<report.size();i++){
		      	  
		      	      if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null)
		      		  total=total+Double.parseDouble(report.get(i).getImpressions());
		           
		              if(report.get(i).getClicks() != null && !report.get(i).getClicks().isEmpty())
		           	  total=total+Double.parseDouble(report.get(i).getClicks());
		            
		              if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
		                total=total+Double.parseDouble(report.get(i).getReach());
		      		 
		            
		              if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null){
		              if(Double.parseDouble(report.get(i).getImpressions())> maxTotal)
		   	    	    {
		   	    	    	maxTotal = Double.parseDouble(report.get(i).getImpressions());
	    	            }
		              }
		        
		              if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty()){
			              if(Double.parseDouble(report.get(i).getClicks())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getClicks());
		    	            }
			              }
		               
		             
		              if(report.get(i).getReach()!=null && !report.get(i).getReach().isEmpty()){
			              if(Double.parseDouble(report.get(i).getReach())> maxTotal)
			   	    	    {
			   	    	    	maxTotal = Double.parseDouble(report.get(i).getReach());
		    	            }
			              }
           
           
           
           }
		                
           /*
           
		         for(int i=0;i<report.size();i++){
		        	      
		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && total !=0.0)
		      	        	 share = (int) round((Double.parseDouble(report.get(i).getImpressions())/total)*100,2);
		      	        
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && total!=0.0)
		      	        	 share =(int) round((Double.parseDouble(report.get(i).getClicks())/total)*100,2);
		      	      
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && total!=0.0)
		      	       	     share =(int) round(( Double.parseDouble(report.get(i).getReach())/total)*100,2);
		      	        	

		        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty() && report.get(i).getAudience_segment()==null && maxTotal!=0.0)
		      	            scaledshare = (int) round((Double.parseDouble(report.get(i).getImpressions())/maxTotal)*100,2);
		      	        	
		      	          
		      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty() && maxTotal!=0.0) 
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getClicks())/maxTotal)*100,2);
		      	        	
		      	        	
		      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty() && maxTotal!=0.0)  
		      	        	  scaledshare = (int) round((Double.parseDouble(report.get(i).getReach())/maxTotal)*100,2);
		      	        	
		      	            
		      	        	report.get(i).setScaledshare(scaledshare.toString());	      	        	 
		      	            report.get(i).setShare(share.toString());
		      	  
		        }
           */
		         
		         for(int i=0;i<report.size();i++){
	        	      
	        	       if(report.get(i).getImpressions()!=null && !report.get(i).getImpressions().isEmpty())
	      	        	 report.get(i).setImpressions(report.get(i).getImpressions());
	      	        
	      	           if(report.get(i).getClicks()!=null && !report.get(i).getClicks().isEmpty())
	      	        	 report.get(i).setClicks(report.get(i).getClicks());
	      	      
	      	           if(report.get(i).getReach() != null && !report.get(i).getReach().isEmpty())
	      	       	     report.get(i).setReach(report.get(i).getReach());
	      	        	
	      	           if(report.get(i).getConversions() != null && !report.get(i).getConversions().isEmpty())
	      	       	     report.get(i).setConversions(report.get(i).getConversions());
	      	        	
	        	     
	      	  
	        }
		         
		         
		         

		      //  Collections.sort(report, new Comparator<Reports>() {
		 				
		 			//	public int compare(Reports o1, Reports o2) {
		 				//	return Double.parseDouble(o1.getScaledshare()) > Double.parseDouble(o2.getScaledshare()) ? -1 : (Double.parseDouble(o1.getScaledshare()) < Double.parseDouble(o2.getScaledshare())) ? 1 : 0;
		 		   //     }
		 		 //   });	
           
           
           return report;
    }
	
    
    
    
    
    
}