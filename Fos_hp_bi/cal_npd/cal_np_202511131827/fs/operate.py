from gateway.operate import get_authorization
from fs.operate_common import listdir, exists, copy_from_local, copy_to_local,\
    open_fs,append, delete, rename, mkdirs, read_df, get_path_meta
from loguru import logger
import time

def call_retry(max_attempts=5, delay=10):
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            error = None
            while attempts < max_attempts + 1:
                try:
                    return func(*args, **kwargs)
                except BaseException as e:
                    error = e
                    logger.error(e)
                    attempts += 1
                    logger.info(f"retry run num: {max_attempts - attempts + 1}")
                    time.sleep(delay)
            logger.error(f"{func.__name__} not success call !!")
            raise BaseException(error)
        return wrapper
    return decorator

class FsClient:
    
    __authorization = None

    def __init__(self, app_key, app_secret):
        self.__authorization = get_authorization(app_key, app_secret)

    @call_retry(max_attempts=2, delay=30)
    def copy_from_local(self, local_path: str, dest_path: str,  overwrite=False):
        copy_from_local(self.__authorization, local_path, dest_path, overwrite)

    @call_retry(max_attempts=2, delay=30)    
    def listdir(self,dest_path):
        return listdir(self.__authorization, dest_path)
    
    
    @call_retry(max_attempts=2, delay=30)    
    def get_path_meta(self,dest_path):
        return get_path_meta(self.__authorization, dest_path)
	
    @call_retry(max_attempts=2, delay=30)    
    def exists(self,dest_path):
        return exists(self.__authorization,dest_path)

    @call_retry(max_attempts=2, delay=30) 		
    def copy_to_local(self,dest_path: str, local_path: str):
        copy_to_local(self.__authorization, dest_path, local_path)

    @call_retry(max_attempts=2, delay=30) 
    def open(self,dest_path):
        return open_fs(self.__authorization, dest_path)


    def append(self,dest_path,data):
        append(self.__authorization, dest_path,data)

    # api返回权限问题，刚创建的文件夹
    @call_retry(max_attempts=2, delay=30) 
    def delete(self,dest_path, recursive = False):
        return delete(self.__authorization, dest_path, recursive)

    @call_retry(max_attempts=2, delay=30) 
    def mkdirs(self, dest_path):
        mkdirs(self.__authorization, dest_path)
    
    # api返回权限问题，刚上传的文件
    def rename(self, path: str, destination: str):
        return rename(self.__authorization, path, destination)
    
    @call_retry(max_attempts=2, delay=30)    
    def read_fs(self, path: str, format="tsv", **config):
        return read_df(self.__authorization, path, format, **config)

