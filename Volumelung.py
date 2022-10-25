import psycopg2
import json
import pandas as pd
from pandas import DataFrame
from helper import connect_to_db
import os
import glob
import datetime
from pandas import ExcelWriter
from helper2 import nodule_size

os.chdir('/Users/kinnaripatel/Desktop/k')
path = "/Users/kinnaripatel/Desktop/k"

for filename in glob.glob(os.path.join(path, '*.json')):     
    with open(filename, mode='r') as f:
        data = json.load(f)
        connection = connect_to_db(data['site_info']['db_name'])
        print (data['site_info']['has_facilities'])
        cursor = connection.cursor()
        if data['site_info']['has_facilities'] == True:
            for facility in data['facility_info']:
                postgreSQL_select_Query_with_facility = """select messages."messageId","{}","dateOfBirth","patientVisitFacility","{}","universalServiceIdentifier1","{}","{}","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality" 
from messages inner join spacy_incidentals as ipn on messages."messageId" = ipn."messageId" 
where report is not null and "esIndex" ilike '{}' and  "{}" >=  CURRENT_DATE - INTERVAL '12 months'""".format(facility['mrn_column_id'],facility['procedure_desc_column_id'],facility['exam_date_column_id'],facility['accession_column_id'],facility['facility_id'], facility['exam_date_column_id'])

                
                cursor.execute(postgreSQL_select_Query_with_facility)
                print("Selecting rows from exams table")
                exams = cursor.fetchall()
                df=pd.DataFrame(list(exams))
                file_name = facility['facility_id']+'.xlsx'
                df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Procedure Code","Exam Date","Accession Number","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality"]
                df.to_csv('sample'+facility['facility_id']+'.csv')
                #df['calc']= df['max_measure'].apply[nodule_size]
                df['size_category'] = df["max_measure"].apply(nodule_size)
                print(df['size_category'])
                df1=df.groupby('size_category')['size_category'].count()
                print(df1)
                df1.to_csv('sample1'+facility['facility_id']+'.csv')
        else:
            postgreSQL_select_Query = """select messages."messageId","{}","dateOfBirth","patientVisitFacility","{}","universalServiceIdentifier1","{}","{}","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality" 
from messages inner join spacy_incidentals as ipn on messages."messageId" = ipn."messageId" 
where report is not null and  "{}" >=  '2021-05-01' and "{}" < '2021-06-01' """.format(data['site_info']['mrn_column_id'],data['site_info']['procedure_desc_column_id'],data['site_info']['exam_date_column_id'],data['site_info']['accession_column_id'],data['site_info']['exam_date_column_id'],data['site_info']['exam_date_column_id'])

            cursor.execute(postgreSQL_select_Query)
            print("Selecting rows from exams table")
            exams1 = cursor.fetchall()
            df2=pd.DataFrame(list(exams1))
            filename = data['site_info']['site_id']
            df2.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Procedure Code","Exam Date","Accession Number","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality"]
            df2.to_csv('sample'+filename+'.csv')
            df2['size_category'] = df2["max_measure"].apply(nodule_size)
            #print(df['size_category'])
            df3=df2.groupby('size_category')['size_category'].count()
            print(df3)
            df3.to_csv('sample1'+filename+'.csv')
