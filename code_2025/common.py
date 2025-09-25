from mlpconn import mlpapp
from gateway.client import Client
import clickhouse_connect
from all_configs.database_config import *


class ClientWrapper:
    def __init__(self, client_type:str, clickhouse_token=None):
        """
        初始化客户端
        :param client_type: 客户端类型，可选值：'gateway', 'clickhouse', 'mysql'
        """
        # 初始化属性
        self.gateway_client = None
        self.fs_client = None
        self.hbase_client = None
        self.mysql_engine = None
        self.clickhouse_client = None
        
        # 初始化客户端
        if client_type == 'gateway':
            self.initialize_gateway_client()
            self.initialize_fs_client()
            self.initialize_hbase_client()
        elif client_type == 'clickhouse':
            self.initialize_clickhouse_client(clickhouse_token)

    def initialize_gateway_client(self):
        """初始化 Gateway 客户端"""
        try:
            self.gateway_client = Client(
                app_key=App_key,
                app_secret=App_secret,
                env=env
            )
            logger.info("Gateway client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Gateway client: {e}")
            raise ValueError("Failed to initialize Gateway client") from e

    def initialize_fs_client(self):
        """初始化 FileSystem 客户端"""
        try:
            if not self.gateway_client:
                raise ValueError("Gateway client is not initialized.")
            self.fs_client = self.gateway_client.getFsClient()
            logger.info("FileSystem client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing FileSystem client: {e}")
            raise

    def initialize_hbase_client(self):
        """初始化 HBase 客户端"""
        try:
            if not self.gateway_client:
                raise ValueError("Gateway client is not initialized.")
            self.hbase_client = self.gateway_client.getHbaseClient(
                fs_root_dir=os.getenv("FS_ROOT_DIR")
            )
            logger.info("HBase client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing HBase client: {e}")
            raise

    def initialize_clickhouse_client(self, clickhouse_token):
        """初始化 ClickHouse 客户端 token"""
        try:
            self.clickhouse_client = self.retry_ck_connection(5, 5, clickhouse_token)
            logger.info("ClickHouse client initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing ClickHouse client: {e}")
            raise
    
    # def initialize_clickhouse_client(self, clickhouse_token):
    #     """初始化 ClickHouse 客户端 账号密码"""
    #     self.clickhouse_client = clickhouse_connect.get_client(
    #             host=clickhouse_connect_params["CLICKHOUSE_HOST"],
    #             user=clickhouse_connect_params["CLICKHOUSE_USER"],
    #             password=clickhouse_connect_params["CLICKHOUSE_PASSWORD"],
    #             port=clickhouse_connect_params["CLICKHOUSE_PORT"],
    #             database=clickhouse_connect_params["CLICKHOUSE_DB"],
    #             query_limit=0
    #         )

    def retry_ck_connection(self, max_retries, delay_minutes, token):
        retries = 0
        while retries < max_retries:
            try:
                res_clickhouse = mlpapp.get_clickhouse_client(token)
                if res_clickhouse.get('successful') == False:
                    raise BaseException(res_clickhouse.get('err_message'))
                clickhouse_client = res_clickhouse.get('object')
                return clickhouse_client
            except BaseException as e:
                logger.info(f"Connection attempt {retries + 1} failed: {str(e)}")
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying in {delay_minutes} minutes...")
                    time.sleep(delay_minutes * 60)
                else:
                    logger.info("Max retries reached, failing...")
                    raise


class ImprovedEmailSender:
    def __init__(self, sender: str, receivers: List[str], subject: str):
        self.sender = sender
        self.receivers = receivers
        self.subject = subject

    def send_email(self, content: any, content_type: str = 'html',
                   new_subject: Optional[str] = None, from_: Optional[str] = None,
                   to_: Optional[str] = None):
        message = MIMEMultipart()
        message['From'] = Header(from_ if from_ else 'COT 2025 to clickhouse & hbase MINTORING', 'utf-8')
        message['To'] = Header(to_ if to_ else ", ".join(self.receivers), 'utf-8')
        message['Subject'] = Header(new_subject if new_subject else self.subject, 'utf-8')

        message.attach(MIMEText(content, content_type, 'utf-8'))

        try:
            smtp_obj = smtplib.SMTP('Internalmail.effem.com')
            smtp_obj.sendmail(self.sender, self.receivers, message.as_string())
            logger.info("邮件发送成功")
        except smtplib.SMTPException as e:
            logger.error(f"SMTP错误，无法发送邮件：{e}")
        except Exception as e:
            logger.error(f"未知错误，无法发送邮件：{e}")
            logger.error(traceback.format_exc())
        finally:
            smtp_obj.quit()

    @staticmethod
    def email_body(error_prefix, error_detail):
        error_message = f"""
            <html>
            <head>
            <style>
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th, td {{
                    border: 1px solid black;
                    padding: 8px;
                    text-align: left;
                }}
            </style>
            </head>
            <body>
                <table>
                    <tr>
                        <th>错误详情: {error_prefix}</th>
                    </tr>
                    <tr>
                        <td><pre>{error_detail}</pre></td>
                    </tr>
                </table>
            </body>
            </html>
            """
        return error_message
            




def return_pipeline_result(metrics: list)-> dict:
    result = {
        "__methods__": [
            {
                "method": "put",
                "scope": "inst",
                "level": "task",
                "body": {
                    "metrics": metrics
                }
            }
        ]
    }
    return result