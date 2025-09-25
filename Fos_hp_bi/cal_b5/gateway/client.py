import os

   
class GateWayClient:
    __app_key = None
    __app_secret  = None
    def __init__(self, app_key, app_secret, env=None):
        self.__app_key = app_key
        self.__app_secret = app_secret
        running_env = env
        if env is not None:
            os.environ['running_env'] = env
        else:
            running_env = os.environ.get('running_env') or 'qa'
            os.environ['running_env'] = running_env
        
           
            
    def getHbaseClient(self, fs_root_dir=None):
        from hbase.operate import HbaseClient
        if fs_root_dir is None:
            fs_root_dir = f"/datahub/intermediate/hbase_operate/{self.__app_key}"
        return HbaseClient(self.__app_key, self.__app_secret, fs_root_dir)
    
    def getFsClient(self):
        from fs.operate import FsClient
        return FsClient(self.__app_key, self.__app_secret)
    