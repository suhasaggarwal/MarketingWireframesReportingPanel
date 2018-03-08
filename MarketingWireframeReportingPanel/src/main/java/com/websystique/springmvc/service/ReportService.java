package com.websystique.springmvc.service;



import java.util.List;

import com.websystique.springmvc.model.CampaignData;
import com.websystique.springmvc.model.Reports;



public interface ReportService {
	
	String extractReports(String queryfield,String metric, String dateRange, String campaignId,String channel, String aggregate,String ratiometric, String download);
	 
	List<CampaignData> extractCuberootCampaignIds(String dateRange);

	List<CampaignData> extractCampaignIds(String dateRange,String cuberootCampaignId);

	String extractCampaignName(String campaignId);
	
	String extractCampaignCurrency(String campaignId);
}
