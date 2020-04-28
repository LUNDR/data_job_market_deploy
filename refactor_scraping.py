# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 14:23:09 2020
Script to scrape indeed uk and push into database
@author: lundr
"""

import pandas as pd
import requests, bs4, time
import os
from itertools import cycle
import traceback
import datetime
from datetime import date
from datetime import datetime as dt
import pickle
import numpy as np
import boto3


from config import ACCESS_KEY,SECRET_KEY



def s3_upload(access_key,secret_key, write_path):

    s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
    )
    bucket_resource = s3
    
    filename = write_path
    
    bucket_resource.upload_file(
    Bucket = 'jobadscrape',
    Filename=filename,
    Key=filename
    )





class indeed_scrape_uk:
    def __init__(self,url):
        self.url = url
        self.res = requests.get(self.url)      
        self.soup = bs4.BeautifulSoup(self.res.content, features="lxml")
# =============================================================================
#         self.jobs = []
#         self.salaries = []
#         self.locations = []
#         self.descriptions = []
#         self.date = []
#         self.companies = []
#         self.links = []
#         self.full_description = []
#         self.headlines = []
#         self.salary_low = []
#         self.salary_high=[]
#         self.salary_type = []
#         self.days_posted = []
#         self.imputed_date = []
#         self.salary_av = []
#         self.extract_date = []
# 
# =============================================================================
    
    def print_soup(self):
        return self.soup
        

    def job_title(self): 
        jobs = []
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                for a in div.find_all(name="a", attrs={"data-tn-element":"jobTitle"}):
                    jobs.append(a["title"])
            except:
                   jobs.append("Nothing_found")
        return jobs
 
    def salary(self): 
        
        salaries = []
    
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                salaries.append(div.find(name="span",attrs={"class":"salaryText"}).text)
            except:
                salaries.append("Nothing_found")
        return salaries

    def location(self):
        locations = []
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                locations.append(div.find("span", attrs={"class": "location accessible-contrast-color-location"}).text)
            except:
                locations.append("Nothing_found")
   
        return locations

    def description(self):
        
        descriptions = []
    
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                descriptions.append(div.find("div", attrs={"class": "summary"}).text)
            except:
                descriptions.append("Nothing_found")
       
        return descriptions

    def posted_date(self): 
        
        dates = []
   
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                dates.append(div.find("span", attrs={"class": "date"}).text)
            except:
                dates.append("Nothing_found")
        return dates

    def company(self): 
        
        companies = []
 
        for div in self.soup.find_all(name="div", attrs={"class":"row"}):
            try:
                companies.append(div.find("span", attrs={"class": "company"}).text)
            except:
                companies.append("Nothing_found")
       
        return companies

    def extract_links(self):
        
        links = []
       
        for div in self.soup.find_all(name='a', attrs={'class':'jobtitle turnstileLink'}):
            links.append('https://www.indeed.co.uk'+str(div['href']))
        return links

    

   
    def clean_salary_GBP(self,string):
        """
         Cleans the salary, removing all text around numbers
        :param pandas_df_col:
        :return: salary as numeric values
        """
        a = string
        a = a.split("£",1)[1]
        a = a.split("a",1)[0]
        a = a.replace(",","")
        a = float(a)

        return a
    
    
    
    def salary_split(self):
        """
        splits the salary column into the upper and lower ranges and whether its per, day, week, year etc.
        where there is only one salary value, upper and lower values are allocated as the same.
        :param pandas_df_col:
        :return: column text in string format
        """
        
        salary_low = []
        salary_high = []
        salary_type = []
        
        salary = self.salary()
        for i in range(len(salary)):
            try:

                a = salary[i].split(" - ", 1)[0]
                a = self.clean_salary_GBP(a)

            except:
                a = 'None'
                
            try:
                b = salary[i].split(" - ", 1)[1]
                b = self.clean_salary_GBP(b)
            except:
                b = a
                
            
            try:
                c = salary[i].split("a",1)[1]
            except:
                c = 'None'

            salary_low.append(a)
            salary_high.append(b)
            salary_type.append(c)
        
        
        return salary_low, salary_high, salary_type 
    
    def full_desc(self):
        links = self.extract_links()
        full_description = []
        for i in range(len(links)):
            res_sub = requests.get(links[i])
            soup_sub = bs4.BeautifulSoup(res_sub.content, features="lxml")
            text=[x.text for x in soup_sub.find_all(name="div",attrs={"id":"jobDescriptionText"})]
            full_description.append(text)
        return full_description

    def headline(self): 
        links = self.extract_links()
        headlines = []
        for i in range(len(links)):
            res_sub = requests.get(links[i])
            soup_sub = bs4.BeautifulSoup(res_sub.content, features="lxml")
            try:
                vals=[x.text for x in soup_sub.find_all(name="span",attrs={"class":"jobsearch-JobMetadataHeader-iconLabel"})]
                a ='_'.join(vals)
                headlines.append(a)
            except:
                headlines.append("Nothing_found")
        return  headlines


    def days_from_post(self):
        """
        Extracts the number of days ago the job was posted, removing surrounding text
        :param pandas_df_col:
        :return: days as string
        """
        a  = self.posted_date()
        
        days_posted = []
        for i in range(len(a)):
            if (a[i] == 'Today')|(a[i] == 'Just posted') :
                days_posted.append(0)
            elif a[i] == 'Nothing_found':
                days_posted.append('None')
            else:
                b = a[i].split("d",1)[0]
                days_posted.append(b.split("+",1)[0])
        return days_posted

    def extraction_date(self):
        
        extract_date = []
        number_of_times = self.job_title()
        
        for i in range(len(number_of_times)):
        
            extract_date.append(date.today())
            
        return extract_date
        


    def imputed_posted_date(self):
        """
        Calculates the estimated day on which the job was posted. "30+" is treated as 30 days
        :param days_from_post_col,extraction_date_col :
        :return: date
        """
        imputed_date = []
        a = self.days_from_post()
        b = self.extraction_date()
        for x in range(len(a)):
            if a[x] is not 'None':
                 imputed_date.append((b[x] - datetime.timedelta(days=int((a[x])))))
            else:
                imputed_date.append('None')
        return imputed_date



    def salary_average(self):
        """
        Calculates the average of the salary range as a integer
        :param days_from_post_col,extraction_date_col :
        :return: a new variable salary average as an integer
        """
        low,high = self.salary_split()[0:2]
        salary_av = []
    
        for i in range(len(low)):
            if low[i] is not 'None':
                salary_av.append(int((low[i] +high[i])/2))
            else:
                salary_av.append('None')           
        return salary_av
    

    

if __name__ == "__main__":
    #Define variables
    
    st = dt.now()
    
    text_file = open("jobs_list.txt", "r")
    list1 = text_file.readlines()
    jobs_list = [x.split('\n')[0] for x in list1]
    
    jobs = jobs_list
    

    
    for searchTerm in jobs:
        print(searchTerm)
        master_df = pd.DataFrame(columns = ['company' , 
                                                     'job_title',
                                                     'salary',
                                                     'location',
                                                     'description',
                                                     'posting_date',
                                                     'extraction_date',
                                                     'full_description',                                                    
                                                     'salary_low',
                                                     'salary_high',
                                                     'salary_type',
                                                     'salary_average',
                                                     'other_deets'])
    

        for i in range(0,1000,10):
            try:
                print(i)
                time.sleep(1) #ensuring at least 1 second between page grabs
                url = "https://www.indeed.co.uk/jobs?q="+searchTerm+"&sort=date&filter=0&l="+"&start="+str(i)
                a = indeed_scrape_uk(url)
                
            
                cols = [a.company(),
                    a.job_title(), 
                    a.salary(),
                    a.location(),
                    a.description(), 
                    a.imputed_posted_date(), 
                    a.extraction_date(),
                    a.full_desc(),
                    a.salary_split()[0],
                    a.salary_split()[1],
                    a.salary_split()[2],
                    a.salary_average(),
                    a.headline()]
     
    
            
                columns = ['company' , 
                                                         'job_title',
                                                         'salary',
                                                         'location',
                                                         'description',
                                                         'posting_date',
                                                         'extraction_date',
                                                         'full_description',                                                    
                                                         'salary_low',
                                                         'salary_high',
                                                         'salary_type',
                                                         'salary_average',
                                                         'other_deets']
            
                df = pd.DataFrame.from_dict(dict(zip(columns, cols)))
                master_df = master_df.append(df, ignore_index=True)
                master_df.drop_duplicates(subset ='description',inplace = True)
            except:
                pass
        
        master_df = master_df.replace({'\n':''}, regex=True)
        current_path = os.getcwd()
        filename='Indeed_UK_scrape_'+searchTerm+'_'+str(date.today())+'.tsv'
        master_df.to_csv(filename,sep='\t', mode='w', encoding = 'UTF-8')
                   
            
        access_key = ACCESS_KEY
        secret_key = SECRET_KEY
            
        s3_upload(access_key,secret_key, filename)  
         
        os.remove(filename)
        print(dt.now() - st)
          
          
          