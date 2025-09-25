from gateway.operate import get_authorization
from fs.operate_common import listdir, exists, copy_from_local, copy_to_local, open_fs,append, delete, rename, mkdirs



class FsClient:
    
    __authorization = None

    def __init__(self, app_key, app_secret):
        self.__authorization = get_authorization(app_key, app_secret)


    def copy_from_local(self, local_path: str, dest_path: str,  overwrite=False):
        copy_from_local(self.__authorization, local_path, dest_path, overwrite)

    def listdir(self,dest_path):
        return listdir(self.__authorization, dest_path)
    
    def exists(self,dest_path):
        return exists(self.__authorization,dest_path)
    
    def copy_to_local(self,dest_path: str, local_path: str):
        copy_to_local(self.__authorization, dest_path, local_path)

    def open(self,dest_path):
        return open_fs(self.__authorization, dest_path)


    def append(self,dest_path,data):
        append(self.__authorization, dest_path,data)

    # api返回权限问题，刚创建的文件夹
    def delete(self,dest_path, recursive = False):
        return delete(self.__authorization, dest_path, recursive)


    def mkdirs(self, dest_path):
        mkdirs(self.__authorization, dest_path)
    
    # api返回权限问题，刚上传的文件
    def rename(self, path: str, destination: str):
        return rename(self.__authorization, path, destination)

