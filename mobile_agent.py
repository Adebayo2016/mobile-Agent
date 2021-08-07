from simplesql.db_queries import DbQueries as dbq
import urllib.request
import os
from google.cloud import storage
class File():
    gcp_storage_credentials ='' #your credentials
    gcp_storage_credentials2 = ''
    file2 = str(os.path.dirname(os.path.realpath(__file__))) + '/' + gcp_storage_credentials
    storage_cl = storage.Client.from_service_account_json(file2)
    def __init__(self,path=os.path.dirname(os.path.realpath(__file__))):
        self.path=path
    def create_bucket(self,bucket_name):
        """Creates a new bucket."""
        bucket = self.storage_cl.create_bucket(bucket_name)
        print('Bucket {} created'.format(bucket.name))

class BucketGcp(File):
    bucket_names = ['analysed_images_live' ,'documentsuploadstest','test_bucket007','livedocs']
    def __init__(self,buck_id):
        self.bucket = self.storage_cl.get_bucket(self.bucket_names[buck_id])

def connect():
    try:
        urllib.request.urlopen('http://google.com') #Python 3.x
        return True
    except:
        return False

class AgentAnalyser:
    def __init__(self,**keyword_args):
        db1 = dbq(database='request_details.db')
        self.data = db1.select_data(table_name='REQUEST_DETAILS',)
    def verify_request(self,**keyword_args):
        self.user_name = keyword_args.get('user_name')
        self.password = keyword_args.get('password')
        permission = False
        security_id = None
        for i in self.data:
            if (self.user_name in i) and (self.password in i) :
                permission = True
                security_id = i[-1]
        return permission, security_id

class SecurityAgent:
    def __init__(self,**keyword_args):
        db1 = dbq(database='data_details.db')
        self.data = db1.select_data(table_name='DATA_DETAILS',)
    def verify_security(self,**keyword_args):
        self.security_id = keyword_args.get('security_id')
        self.system_id = keyword_args.get('system_id')
        permission = False
        user_data_ = None
        for i in self.data:
            if (self.security_id in i) and (self.system_id in i) :
                permission = True
                user_data = i
        return permission, user_data

class Cloud:
    def __init__(self,buck_id=2):
        self.cursor = BucketGcp(buck_id) 
        self.allowed_user_access = '' # restrict query based on thius guy

    def create_record(self):
        self.cursor # create method

    def read_record(self):
        self.cursor # read method

    def update_record(self):
        self.cursor # update method

    def delete_record(self):
        self.cursor # delete method


class Mobile_System:

    def __init__(self,**kwargs):
        self.access = kwargs.get('access')
        self.user_name = kwargs.get('user_name')
        self.password = kwargs.get('password')
        self.system_id = kwargs.get('system_id')

        task_manager = mediator_agent()
        if task_manager[0]:
            security_agent = SecurityAgent()
            data = {'security_id':task_manager[1],'system_id':self.system_id}
            security_agent = security_agent.verify_security()
            if security_agent:
                print(security_agent[1]) # call cloud service
                if connect():
                    print('calling cloud services')
                else:
                    print('using local database and creating a cronjob to check internet connection')
            else:
                print('security check failed')

    def mediator_agent(self):
        data = {'user_name':self.user_name,'password':self.password}
        agent_analyser = AgentAnalyser()
        response = agent_analyser.verify_request(**data)
    
    
