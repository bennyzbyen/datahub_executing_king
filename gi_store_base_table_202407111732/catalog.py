import os.path
from plugin_config import io_column_dict

from hbase.operate import HbaseClient


class DataCatalog(object):
    def __init__(self, hbase_client: HbaseClient, logger: any, local_export_path: str = 'export_files'):
        self.logger = logger
        self.hb_client = hbase_client
        self.local_export_path = local_export_path

    def get_config(self, table_name: str):
        return io_column_dict[table_name]

    def load_data_from_hbase(self, table_name: str, period: str = None, from_local: bool = False) -> list:
        """
        :param period: Example: 202406

        :return:
        """
        files = []
        local_export_path = self.local_export_path
        logger = self.logger
        hb_client = self.hb_client
        hb_table_name = table_name
        hb_columns = self.get_config(table_name)
        if from_local:
            FROM_HBASE = False
            if os.path.exists(local_export_path):
                target_dirs = [i for i in os.listdir(local_export_path) if i.startswith(hb_table_name) and not i.startswith(f"{hb_table_name}_freeze")]
                if len(target_dirs) <= 0:
                    FROM_HBASE = True
                else:
                    target_dirs.sort()
                    target_dir = target_dirs[-1]
                    logger.info(f"Local dirs found, use latest one: {target_dir}")
                    files = [f"{local_export_path}{os.sep}{target_dir}{os.sep}{i}" for i in os.listdir(f"{local_export_path}{os.sep}{target_dir}")]
            else:
                FROM_HBASE = True

            if FROM_HBASE:
                logger.info("Directory 'export_files' does not exist. Export from Hbase now.")
                if period is None:
                    files = hb_client.async_export_table(hbase_table_name=hb_table_name, columns=hb_columns, )
                else:
                    files = hb_client.async_export_table(hbase_table_name=hb_table_name, columns=hb_columns,
                                                         row_start=period, row_stop=f"{period}Z",
                                                         row_prefixs=[str(i) for i in range(0, 10)])
        else:
            if period is None:
                files = hb_client.async_export_table(hbase_table_name=hb_table_name, columns=hb_columns)
            else:
                files = hb_client.async_export_table(hbase_table_name=hb_table_name, columns=hb_columns,
                                                     row_start=period, row_stop=f"{period}Z",
                                                     row_prefixs=[str(i) for i in range(0, 10)])
        return files
