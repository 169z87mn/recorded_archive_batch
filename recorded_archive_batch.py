import os
import shutil
from dataclasses import dataclass
from typing import Optional, List
from dotenv import load_dotenv
import mysql.connector

DEFAULT_CACHE_DAYS = '7'
TS_DATETIME_FORMAT = '%Y年%m月%d日%H時%i分%s秒'

class Env:
    def __init__(self) -> None:
        load_dotenv()
        self.cache_storage = os.getenv('CACHE_STORAGE_NAME')
        self.cache_recorded_folder = os.getenv('CACHE_RECORDED_FOLDER')
        self.archive_storage = os.getenv('ARCHIVE_STORAGE_NAME')
        self.archive_recorded_folder = os.getenv('ARCHIVE_RECORDED_FOLDER')
        self.cache_expire_days = os.getenv('CACHE_EXPIRE_DAYS', DEFAULT_CACHE_DAYS)
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = os.getenv('DB_HOST')


class Mysqlhandler:
    def __init__(self) -> None:
        env = Env()
        self.cnx = mysql.connector.connect(user=env.db_user,
                                           password=env.db_password,
                                           host=env.db_host)
        self.cursor = self.cnx.cursor()


    def __enter__(self):
        self.__init__()


    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()


class ArchivingRecordings:
    @staticmethod
    def run(env: Env, db_conn: Mysqlhandler):
        for video_file in VideoFileModel.find_cache_expired(db_conn, env.cache_expire_days, env.cache_storage):
            src = os.path.join(env.cache_recorded_folder, video_file.name)
            dst = os.path.join(env.archive_recorded_folder, video_file.name)
            try:
                shutil.move(src, dst)
            except Exception as e:
                print(e)
                continue

            try:
                VideoFileModel.update_path(db_conn, video_file.recorded_id, env.archive_storage)
            except Exception as e:
                print(e)
                shutil.move(dst, src)

@dataclass
class VideoFile:
    recorded_id: str
    name: str

class VideoFileModel:
    @classmethod
    def find_cache_expired(self,
                           db_conn: Mysqlhandler,
                           cache_expire_days: str,
                           cache_storage: str) -> Optional[List[VideoFile]]:
        sql = """
            SELECT 
                id,
                filePath,
                parentDirectoryName,
                rec_date,
                expire_date
            FROM
                (
                    SELECT
                        id,
                        filePath,
                        parentDirectoryName,
                        STR_TO_DATE(filePath, %s) as rec_date,
                        DATE_ADD(NOW(), INTERVAL -%s DAY) as expire_date
                    FROM 
                        epgstation.video_file
                ) as vft
            WHERE
                parentDirectoryName = %s AND
                rec_date <= expire_date
        """
        db_conn.cursor.execute(sql, (TS_DATETIME_FORMAT, cache_expire_days, cache_storage))
        return [VideoFile(**{'recorded_id': str(r[0]), 'name': r[1]}) for r in db_conn.cursor.fetchall()]


    @classmethod
    def update_path(self, db_conn: Mysqlhandler, recorded_id: str, parent_directory_name: str):
        sql = "UPDATE epgstation.video_file SET parentDirectoryName = %s WHERE id = %s"
        db_conn.cursor.execute(sql, (parent_directory_name, recorded_id))
        db_conn.cnx.commit()


if __name__ == "__main__":
    with Mysqlhandler() as db_conn_:
        ArchivingRecordings.run(Env(), db_conn_)
