package com.websystique.springmvc.service;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.websystique.springmvc.model.CampaignData;
import com.websystique.springmvc.model.Reports;

@Service("reportService")
@Transactional
public class ReportServiceImpl implements ReportService{
	
	private static final AtomicLong counter = new AtomicLong();
	
	ReportDAOImpl repDAO = ReportDAOImpl.getInstance();
	
	String report = null;
	
	List<CampaignData> campaignIds = new ArrayList<CampaignData>();
	
    String campaignId = null;
	
    public String extractReports(String queryfield,String metric,String dateRange,String campaignId,String channel, String aggregate,String ratiometric, String download){
	
        String [] dateInterval = dateRange.split("_");
    	
    /*	
    	if(id == 1){
    	 	report=repDAO.FetchImpressionsData(metric,dateInterval[0], dateInterval[1], campaignId);
		    return report;
    	}
    	
    	
    	if(id == 2){
    	 	report=repDAO.FetchClicksData(metric,dateInterval[0], dateInterval[1], campaignId);
		    return report;
    	}
    	
    	
    	if(id == 3){
    	 	report=repDAO.FetchConversionsData(metric,dateInterval[0], dateInterval[1], campaignId);
		    return report;
    	}
    	
    	
    	if(id == 4){
    	 	report=repDAO.FetchCostData(dateInterval[0], dateInterval[1], campaignId);
		    return report;
    	}
    	
    	*
    	*
    	*/
        
        if(queryfield.equals("metricdatadatewise"))
        {
        	
        report = repDAO.FetchMetricsDataDatewise(dateInterval[0], dateInterval[1], campaignId,channel,metric,download);
        return report;
        	
        	
        }
    	
        if(queryfield.equals("metricdata"))
        {
        	
        report = repDAO.FetchMetricsData(dateInterval[0], dateInterval[1], campaignId,channel,aggregate,metric,download);
        return report;
        	
        	
        }
        
        if(queryfield.equals("duration"))
        {
        	
        report = repDAO.FetchDurationData(dateInterval[0], dateInterval[1], campaignId);
        return report;
        	
        	
        }
        
    	if(queryfield.equals("audience_segment")){
    	 	
    		report=repDAO.FetchAudienceSegmentImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	
    	if(queryfield.equals("city")){
    	 	report=repDAO.FetchCityImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	
    	if(queryfield.equals("cityOthers")){
    	 	report=repDAO.FetchCityOthersImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	
    	
    	if(queryfield.equals("device")){
    	 	report=repDAO.FetchDeviceImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
        
    	if(queryfield.equals("os")){
    	 	report=repDAO.FetchOSImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	
    	if(queryfield.equals("agegroup")){
    	 	report=repDAO.FetchAgeImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
        
    	if(queryfield.equals("gender")){
    	 	report=repDAO.FetchGenderImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	  	
    	if(queryfield.equals("income")){
    	 	report=repDAO.FetchIncomeImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
        
    	if(queryfield.equals("brand")){
    	 	report=repDAO.FetchBrandImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	if(queryfield.equals("isp")){
    	 	report=repDAO.FetchISPImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	if(queryfield.equals("screensize")){
    	 	report=repDAO.FetchScreenSizeImpData(metric,dateInterval[0], dateInterval[1], campaignId,channel,aggregate,download);
		    return report;
    	}
    	
    	return report;
    

    }
		

    public List<CampaignData> extractCuberootCampaignIds(String dateRange){
      
    	  String [] dateInterval = dateRange.split("_");

         
           campaignIds = repDAO.extractCuberootCampaignIds(dateInterval[0], dateInterval[1]);
           return campaignIds;
          
    
    
    }

    
    
    public List<CampaignData> extractCampaignIds(String dateRange,String cuberootcampaignId){
        
  	  String [] dateInterval = dateRange.split("_");

       
         campaignIds = repDAO.extractCampaignIds(dateInterval[0], dateInterval[1],cuberootcampaignId);
         return campaignIds;
        
  
  
  }

    
    public String extractCampaignName(String campaignId){
        
 	   
        String campaignName =  ReportDAOImpl.campaignIdcampaignNameMap.get(campaignId);
       
        return campaignName;
 
 }  
    

    public String extractCampaign(String campaignId){
        
  	   
        String campaignName =  ReportDAOImpl.campaignIdcampaignNameMap.get(campaignId);
       
        return campaignName;
 
 }  
    
    public String extractCampaignCurrency(String campaignId){
    	
    	String currency = repDAO.FetchCurrencyData(campaignId);
       
    	return currency;
       
    }

}
