from common_utils.all_modules import *


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


class ImprovedEmailSender:
    def __init__(self, sender: str, receivers: List[str], subject: str):
        self.sender = sender
        self.receivers = receivers
        self.subject = subject

    def send_email(self, content: any, content_type: str = 'html',
                   new_subject: Optional[str] = None, from_: Optional[str] = None,
                   to_: Optional[str] = None):
        message = MIMEMultipart()
        message['From'] = Header(from_ if from_ else 'Fos-HQ BI Execute King MINTORING', 'utf-8')
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
    

def process_sku_config(params: dict) -> pd.DataFrame:
    """
    根据输入的参数字典处理SKU配置信息，生成最终的配置DataFrame。

    Args:
        params (dict): 包含SKU映射、过滤规则和激活状态的字典。
                       - 'sku_map': {sku_code: sku_name}
                       - 'sku_ttl_filter': {sku_code: [excluded_channels]}
                       - 'sku_is_active': {sku_code: bool}

    Returns:
        pd.DataFrame: 包含最终SKU配置信息的DataFrame。
    """
    try:
        # 1. 从参数中提取数据
        sku_map = params.get('sku_map', {})
        sku_ttl_filter = params.get('sku_ttl_filter', {})
        sku_is_active = params.get('sku_is_active', {})

        if not sku_map:
            return pd.DataFrame()

        # 2. 定义渠道常量和基础DataFrame
        all_channels = ['Hyper', 'Super', 'Mini', 'CVS', 'Non KA MT', 'TT']
        all_channels_set = set(all_channels)
        
        # 直接从 sku_map 创建基础DataFrame，避免后续合并
        df = pd.DataFrame(sku_map.items(), columns=['sku_code', 'sku_name'])

        # 3. 计算评估渠道 (assess_channel)
        # 修正 sku_ttl_filter 中的 sku_code ('y' -> 'f')，以便与 sku_map 匹配
        transformed_ttl_filter = {k.replace('y', 'f'): v for k, v in sku_ttl_filter.items()}
        
        # 定义一个函数来计算每个SKU应包含的渠道
        def get_included_channels(sku_code):
            # 使用 .get(sku, []) 如果SKU不在过滤字典中，则返回空列表（即不剔除任何渠道）
            excluded = set(transformed_ttl_filter.get(sku_code, []))
            return list(all_channels_set - excluded)

        # 使用 .map() 高效地应用上述函数，并将结果列表转换为字符串
        df['assess_channel'] = df['sku_code'].map(get_included_channels).astype(str)

        # 4. 映射激活状态 (is_active)
        # 直接将 is_active 字典映射到 'sku_code' 列，然后转换 T/F 为 Y/N
        df['is_active'] = df['sku_code'].map(sku_is_active).map({True: 'Y', False: 'N'})

        # 5. 添加剩余列
        df['sku_type'] = 'new_sku'
        df['display_order'] = np.arange(1, len(df) + 1)

        # 6. 按要求选择并排序列
        final_cols = ['sku_code', 'sku_name', 'display_order', 'is_active', 'sku_type', 'assess_channel']
        
        return df[final_cols]
    except Exception as e:
        df = None
        logger.error(f"process_sku_config error: {e}")
        return df