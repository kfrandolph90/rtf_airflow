# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#   Some helpful wrappers for dfa reporting.
#   RTF - Kyle.Randolph@essenceglobal.com
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

from googleapiclient import discovery, http
from oauth2client import client
from io import FileIO
import logging
import csv


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
        self.report = self.get_report(profile_id,report_id)        
        
        try:
            self.file = self.get_last_file(profile_id,report_id)        
            self.file_id = self.file.get('id')
        except:
            self.file = None
    def get_report(self,profile_id,report_id):
        request = self.service.reports().get(profileId=profile_id,reportId=report_id)
        resp = request.execute()
        return resp
    
    def get_last_file(self,profile_id,report_id):
        request = self.service.reports().files().list(profileId=profile_id,reportId=report_id,maxResults=1)
        resp = request.execute()
        return resp['items'][0]
    
    def run_report(self):
        request = self.service.reports().run(profileId=self.profile_id,reportId=self.report_id)
        resp = request.execute()
        self.file = resp        
    
    def patch_report(self,patched_report):
        request = self.service.reports().patch(profileId=self.profile_id,reportId=self.report_id, body=patched_report)
        resp = request.execute()
        self.report = resp
        
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
        request = self.service.files().get(reportId=self.report_id,fileId=self.file_id)
        report_file = request.execute()

        file_name = report_file['fileName'] or report_file['id']

        if report_file['format'] == 'CSV': 
            extension = '.csv' 
        else:
            extension = '.xml'

        file_name = file_name + extension

        if report_file['status'] == 'REPORT_AVAILABLE':
            out_file = FileIO(file_name, mode='wb')

            request = self.service.files().get_media(reportId=self.report_id, fileId=self.file_id)

            downloader = http.MediaIoBaseDownload(out_file, request,
                                                chunksize=CHUNK_SIZE)

            download_finished = False

            while download_finished is False:
                _, download_finished = downloader.next_chunk()
        self.file_name = file_name
        return file_name

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