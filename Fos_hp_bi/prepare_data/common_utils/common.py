from mlpconn import mlpapp
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