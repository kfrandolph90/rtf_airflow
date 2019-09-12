# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#   Some helpful wrappers for dfa reporting.
#   RTF - Kyle.Randolph@essenceglobal.com
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

from googleapiclient import discovery, http
from oauth2client import client
from io import FileIO
import logging
import csv
import time

"""
TODO:
 - Implement Module Level Logging
 - Report Cleaning

"""

class CampaignManagerReport:
    """
    Reference:
    131786951123-compute@developer.gserviceaccount.com ----> 5096586
    
    
    
    """
    
    api_name = 'dfareporting'
    api_version = 'v3.3'
    api_scopes = ['https://www.googleapis.com/auth/dfareporting',
                  'https://www.googleapis.com/auth/dfatrafficking',  
                  'https://www.googleapis.com/auth/ddmconversions']
    
    def __init__(self,creds,profile_id,report_id):
        self.profile_id = profile_id
        self.report_id = report_id        
        scoped_creds = creds.with_scopes(self.api_scopes)
        self.service = discovery.build(self.api_name, self.api_version, credentials=scoped_creds,cache_discovery=False)
           
    def get_report(self):
        request = self.service.reports().get(profileId=self.profile_id,reportId=self.report_id)
        resp = request.execute()
        self.report = resp
        self.name = resp['name']
        self.date_range = resp['criteria']['dateRange']
    
    def get_last_file(self):
        request = self.service.reports().files().list(profileId=self.profile_id,reportId=self.report_id,maxResults=1)
        resp = request.execute()
        
        try:
            self.file = resp['items'][0]
            return resp['items'][0]
        except:
            self.file = None
            return None
    
    def run_report(self):
        request = self.service.reports().run(profileId=self.profile_id,reportId=self.report_id)
        resp = request.execute()
        self.file = resp        
    
    def patch_report(self,patched_report):
        request = self.service.reports().patch(profileId=self.profile_id,reportId=self.report_id, body=patched_report)
        resp = request.execute()
        self.get_report(self)
        
        
    def set_date_range(self,start_date,end_date):
        self.report['criteria']['dateRange'] = {"startDate":start_date,"endDate":end_date,"relativeDateRange":None}        
        self.patch_report(self.report)
        
    def get_report_status(self):
        request = self.service.files().get(reportId=self.report_id,fileId=self.file['id'])
        resp = request.execute()
        self.file = resp
        self.status = self.file['status']
        return self.status
    
    def download_file(self):
        CHUNK_SIZE = 32 * 1024 * 1024
        request = self.service.files().get(reportId=self.report_id,fileId=self.file['id'])
        report_file = request.execute()

        file_name = report_file['fileName'] or report_file['id']

        if report_file['format'] == 'CSV': 
            extension = '.csv' 
        else: 
            extension = '.xml'

        file_name = file_name + extension

        if report_file['status'] == 'REPORT_AVAILABLE':
            out_file = FileIO(file_name, mode='wb')

            request = self.service.files().get_media(reportId=self.report_id, fileId=self.file['id'])

            downloader = http.MediaIoBaseDownload(out_file, request,
                                                chunksize=CHUNK_SIZE)

            download_finished = False

            while download_finished is False:
                _, download_finished = downloader.next_chunk()
        self.file_name = file_name
        return file_name

def get_dfa_report(creds,report_id,start_date,end_date,profile_id = 5096586):
    """ 
    Gets DFA report from API and download
  
    Parameters: 
    report_id (int): id of report in dfa platform
    start_date (date): reporting start date (YYYY-MM-DD)
    end_date (date): reporting end date (YYYY-MM-DD)
    
    
    Returns: 
    filename (str): local filename
    
    """
    #instantiate rep
    report = CampaignManagerReport(creds,profile_id = profile_id,report_id = report_id)
    
    #gets report metadata from API
    print("Get Report Meta")
    report.get_report() # 
    
    
    ## check if report date range is current
    if (report.date_range['endDate'] == end_date and 
        report.date_range['startDate'] == start_date):
        ## see if a file has been generated
        report.get_last_file()
    else:
        report.set_date_range(start_date,end_date)
        report.run_report()
    
    
    ## file exists in API, matches report range and is available then just download it
    if (report.file and 
        report.file['dateRange']['endDate'] == end_date and
        report.file['dateRange']['startDate'] == start_date and
        report.file['status'] == 'REPORT_AVAILABLE'):
        print("File is already available. Start download")
        filename = report.download_file()
        print("File downloaded to {}".format(filename))
        
        return filename

    
    ## file doesn't exist in API or doesn't matches report range, run report
    else:
        print("File doesn't exist, run report")
        report.run_report()
    
    ## ok lets download this fucker
    print("The Download Part")
    tries = 0            
    
    if report.file['status'] != 'REPORT_AVAILABLE':
        
        while report.file['status'] == 'PROCESSING':
        
            print("Report Processing, Wait 10. Try {}".format(tries))
            time.sleep(10)            
            report.get_report_status()
            tries +=1

            if report.file['status'] == 'REPORT_AVAILABLE':
                print("Print Report Available!")
                break
        
    filename = report.download_file()
    print("File downloaded to {}".format(filename))
    return filename

def clean_dcm_file(filename):
    data = []
    write = False
    with open(filename,'r') as f:
        reader = csv.reader(f, delimiter=',')

        for row in reader:
            if write == True:
                data.append(row)        
            elif row == ['Report Fields']:
                write = True

        if data[-1][0] == 'Grand Total:':
            data.pop()

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

## Implement saving cleaned file ^^