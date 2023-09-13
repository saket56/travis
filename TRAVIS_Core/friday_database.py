''' 
    Created By: Rohit Abhishek 
    Function: This is database module for TRAVIS to create sqlite database for various operations 
'''


import sqlite3
import threading


class TravisDatabase:
    def __init__(self, dbname) -> None:
        """ initialize """

        self.dbname = dbname
        self.local = threading.local()

    def get_connection(self)-> sqlite3.Connection:
        """ get connection using object's lock """

        if not hasattr(self.local, "connection"):
            self.local.connection = sqlite3.connect(
                self.dbname, check_same_thread=False
            )
        return self.local.connection    
    
    def create_table(self) -> None:
        pass

    def create_base_release_tables(self) -> None:
        pass 

    def get_base_release_data(self) -> None:
        pass 

    def get_base_file_not_in_release(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get base records that are not present in the release database """

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)


        if db_key.upper() == "FILE_NAME":
            base_not_in_release_sql = f""" SELECT A.{db_key.upper()}, "", "", "", "Base File Not Found in Release"
                                            FROM (
                                                    SELECT T1.{db_key.upper()} FROM MAIN.{base_table} AS T1 
                                                                EXCEPT
                                                    SELECT T2.{db_key.upper()} FROM {attach_db_name}.{release_table} AS T2
                                                ) AS A ; """
        else: 
            base_not_in_release_sql = f""" SELECT A.FILE_NAME, "", "", "", "Base File Not Found in Release"
                                            FROM MAIN.{base_table} AS A
                                            INNER JOIN 
                                            (
                                                    SELECT T1.{db_key.upper()} FROM MAIN.{base_table} AS T1 
                                                                EXCEPT
                                                    SELECT T2.{db_key.upper()} FROM {attach_db_name}.{release_table} AS T2
                                            ) AS B 
                                            ON A.{db_key.upper()} = B.{db_key.upper()}; """        

        # common base and release metadata 
        cursor.execute(base_not_in_release_sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    

    def get_release_file_not_in_base(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get rows that are in release but not in base """
        
        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        if db_key.upper() == "FILE_NAME":   
            release_not_in_base_sql = f""" SELECT A.{db_key.upper()}, "", "", "", "Release File not found in Base"
                                            FROM (
                                                    SELECT T2.{db_key.upper()} FROM {attach_db_name}.{release_table} AS T2
                                                                EXCEPT
                                                    SELECT T1.{db_key.upper()} FROM MAIN.{base_table} AS T1                                                        
                                                ) AS A ; """
        else:
            release_not_in_base_sql = f""" SELECT A.FILE_NAME, "", "", "", "Release File Not Found in Base"
                                            FROM {attach_db_name}.{release_table} AS A
                                            INNER JOIN 
                                            (
                                                    SELECT T2.{db_key.upper()} FROM {attach_db_name}.{release_table} AS T2
                                                                EXCEPT
                                                    SELECT T1.{db_key.upper()} FROM MAIN.{base_table} AS T1    
                                            ) AS B 
                                            ON A.{db_key.upper()} = B.{db_key.upper()};"""        

        # common base and release metadata 
        cursor.execute(release_not_in_base_sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows


    def get_common_base_release_count(self) -> None:
        pass

    def get_base_key_id_not_in_release(self) -> None:
        pass 

    def get_release_key_id_not_in_base(self) -> None:
        pass     


    def create_compare_table(self, table_name) -> None:
        """ create pdf base and release tables """
        
        connection = self.get_connection()
        cursor = connection.cursor()

        # drop and create if exists 
        cursor.execute(f""" DROP TABLE IF EXISTS {table_name.upper()}; """ )
        cursor.execute(f""" CREATE TABLE {table_name.upper()} (
                                    KEY_ID                              TEXT, 
                                    FIELD_NAME                          TEXT,
                                    BASE_VALUE                          TEXT,
                                    RELEASE_VALUE                       TEXT,
                                    REMARKS                             TEXT, 
                                    APPLICATION_NAME                    TEXT, 
                                    ENVIRONMENT_NAME                    TEXT); """)
        # Create indexes on this table 
        cursor.execute(f""" CREATE INDEX KEY_ID_INDEX ON {table_name.upper()} (KEY_ID); """)

        # Issue commit and close the connection 
        connection.commit()
        cursor.close()


    def select_data(self, select_sql) -> list:
        """ run sql to retrieve rows from the table """

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows    
    

    def count_rows(self, table_name) -> tuple:
        """ count number of records in given table """

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name.upper()} ;")
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows
    

    def select_sample_data(self, table_name, limit=50) -> tuple:
        """ get sample records from the table with specified limit """

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name.upper()} LIMIT {limit};")
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()        

        return rows
    

    def insert_data(self, table_name, data) -> None:
        """ insert data to the sqlite database """

        connection = self.get_connection()
        cursor = connection.cursor()

        # create INSERT sql
        for row in data:
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({','.join(['?']*len(row))});", row
            )
        connection.commit()
        cursor.close()


    def run_attach_sql(self, attach_db, sql, attach_db_name="RELEASE_DB") -> list:
        """ run query using ATTACH Functionality """

        connection = self.get_connection()
        cursor = connection.cursor()
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows    
    

    def get_count_summary(self, table_name="MISMATCH_TABLE") -> 'list[tuple]':
        """get all data needed for creating html mismatch report"""

        connection = self.get_connection()
        cursor = connection.cursor()

        # get mismatch data for HTML report
        get_field_count_sql = f""" SELECT CASE WHEN (FIELD_NAME IS NULL OR FIELD_NAME = "") 
                                                          AND (REMARKS IS NOT NULL OR REMARKS <> "")
                                                          THEN REMARKS
                                                     ELSE FIELD_NAME END AS FIELD_NAME, 
                                                     COUNT (*) AS COUNTS 
                                    FROM {table_name}
                                    GROUP BY FIELD_NAME 
                                    ORDER BY COUNTS DESC 
                                    LIMIT 50 ; """
        cursor.execute(get_field_count_sql)
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows
    

    def get_mismatch_sample_data(self, table_name="MISMATCH_TABLE", limit=50) -> 'list[tuple]':
        """ get the mismatch sample data """

        connection = self.get_connection()
        cursor = connection.cursor()

        # mismatch data SQL
        get_mismatch_summary_fields = f""" SELECT KEY_ID, 
                                                  FIELD_NAME, 
                                                  BASE_VALUE, 
                                                  RELEASE_VALUE,
                                                  REMARKS
                                             FROM {table_name} """ 
        if limit > 0: 
            get_mismatch_summary_fields =  get_mismatch_summary_fields + f" LIMIT {limit} ; "

        cursor.execute(get_mismatch_summary_fields)
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows



    def disconnect(self) -> None:
        """ disconnect from the database """
        
        connection = self.get_connection()
        connection.close()



class MetadataDatabase(TravisDatabase):
    """ Class for Metadata database objects """

    def  __init__(self, dbname) -> None:
        super().__init__(dbname)
      

    def create_table(self, table_name) -> None:
        """ create Metadata base and release tables """

        connection = self.get_connection()
        cursor = connection.cursor()

        cursor.execute(f""" DROP TABLE IF EXISTS {table_name.upper()}; """ )
        cursor.execute(f""" CREATE TABLE {table_name.upper()} (
                                    FILE_INDEX                          INTEGER,
                                    FILE_PATH                           TEXT,                       
                                    FILE_NAME                           TEXT,
                                    FILE_TYPE                           VARCHAR(10),
                                    FILE_SIZE                           INTEGER, 
                                    FILE_CHECKSUM                       TEXT,
                                    FILE_CREATE_TIMESTAMP               VARCHAR(50),
                                    FILE_MODIFIED_TIMESTAMP             VARCHAR(50)); """)
        
        # Create indexes on this table 
        cursor.execute(f""" CREATE INDEX FILE_INDEX_INDX ON {table_name.upper()} (FILE_INDEX); """)
        cursor.execute(f""" CREATE INDEX FILE_NAME_INDX ON {table_name.upper()} (FILE_NAME); """)
        connection.commit()

        # Issue commit and close the connection 
        connection.commit()
        cursor.close()


    def get_base_release_data(self, attach_db, db_key, base_table="BASE_METADATA_TABLE", release_table="RELEASE_METADATA_TABLE", attach_db_name="RELEASE_DB") -> tuple:
        """ get base and release metadata for given key """

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        # common base and release metadata 
        cursor.execute(f""" SELECT T1.FILE_INDEX, 
                                   T1.FILE_NAME, 
                                   T1.FILE_TYPE,
                                   T1.FILE_SIZE,
                                   T1.FILE_CHECKSUM,                       
                                   T1.FILE_CREATE_TIMESTAMP,
                                   T1.FILE_MODIFIED_TIMESTAMP,
                                   T2.FILE_INDEX, 
                                   T2.FILE_NAME,
                                   T2.FILE_TYPE,
                                   T2.FILE_SIZE,
                                   T2.FILE_CHECKSUM,                       
                                   T2.FILE_CREATE_TIMESTAMP,
                                   T2.FILE_MODIFIED_TIMESTAMP
                            FROM MAIN.{base_table} AS T1 
                            INNER JOIN {attach_db_name}.{release_table} AS T2
                            ON T1.{db_key} = T2.{db_key}; """)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    


    def get_common_base_release_count(self, attach_db, attach_db_name="RELEASE_DB") -> tuple:
        """ get common base and release row count """

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        # execute common count sql 
        cursor.execute(""" SELECT COUNT(MAIN.BASE_TABLE.KEY_ID) 
                             FROM MAIN.BASE_TABLE
                            INNER JOIN RELEASE_DB.RELEASE_TABLE 
                               ON MAIN.BASE_TABLE.KEY_ID = RELEASE_DB.RELEASE_TABLE.KEY_ID; """)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows


class PDFDatabase(TravisDatabase):
    """ Execute SQLite Queries for various operations """

    def __init__(self, dbname) -> None:
        super().__init__(dbname)      


    def create_table(self, table_name) -> None:
        """ create pdf base and release tables """
        
        connection = self.get_connection()
        cursor = connection.cursor()

        # drop and create if exists 
        cursor.execute(f""" DROP TABLE IF EXISTS {table_name.upper()}; """ )
        cursor.execute(f""" CREATE TABLE {table_name.upper()} (
                                    FILE_INDEX                          INTEGER,
                                    FILE_PATH                           TEXT,                       
                                    FILE_NAME                           TEXT,
                                    PAGE_NUMBER                         TEXT,
                                    PAGE_CHECKSUM                       TEXT, 
                                    PAGE_TEXT_CONTENT                   TEXT); """)
        # Create indexes on this table 
        cursor.execute(f""" CREATE INDEX FILE_INDEX_INDX ON {table_name.upper()} (FILE_INDEX); """)
        cursor.execute(f""" CREATE INDEX FILE_NAME_INDX ON {table_name.upper()} (FILE_NAME); """)

        # Issue commit and close the connection 
        connection.commit()
        cursor.close()


    def get_base_page_not_in_release_pdf(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ for found files in both base and release get page level differences - base page not in release"""

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        # create sql statement for comparing the document pages
        base_not_in_release_sql = f""" SELECT A.KEY_FIELD, A.PAGE_NUMBER, "", "", "Page not found in Base"
                                        FROM (
                                                SELECT T2.{db_key.upper()} AS KEY_FIELD, T2.PAGE_NUMBER FROM {attach_db_name}.{release_table} AS T2
                                                            EXCEPT
                                                SELECT T1.{db_key.upper()} AS KEY_FIELD, T1.PAGE_NUMBER FROM MAIN.{base_table} AS T1                                                        
                                        ) AS A ; """


        # common base and release metadata 
        cursor.execute(base_not_in_release_sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    

    def get_release_page_not_in_base_pdf(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ for found files in both base and release get page level differences - release page not in base"""

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        # create sql statement for comparing the document pages
        release_not_in_base_sql = f""" SELECT A.KEY_FIELD, A.PAGE_NUMBER, "", "", "Page not found in Release"
                                        FROM (
                                            SELECT T1.{db_key.upper()} AS KEY_FIELD, T1.PAGE_NUMBER FROM MAIN.{base_table} AS T1 
                                                        EXCEPT
                                            SELECT T2.{db_key.upper()} AS KEY_FIELD, T2.PAGE_NUMBER FROM {attach_db_name}.{release_table} AS T2                                                                                                  
                                        ) AS A ; """

        # common base and release metadata 
        cursor.execute(release_not_in_base_sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    

    def get_matching_page_checksum(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get matching page checksum values """

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)

        # create sql to get matching page checksum rows
        base_release_matching_checksum = f""" SELECT T1.FILE_NAME, 
                                                     T1.PAGE_NUMBER, 
                                                     T1.PAGE_CHECKSUM, 
                                                     T2.PAGE_CHECKSUM,
                                                     "" AS REMARKS
                                                FROM MAIN.{base_table} AS T1 
                                                INNER JOIN {attach_db_name}.{release_table} AS T2
                                                ON T1.{db_key} = T2.{db_key}
                                                AND T1.PAGE_NUMBER = T2.PAGE_NUMBER
                                                AND T1.PAGE_CHECKSUM = T2.PAGE_CHECKSUM
                                                ; """

        # common base and release metadata 
        cursor.execute(base_release_matching_checksum)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows


    def get_unmatching_page_checksum(self, attach_db:tuple=(), db_key:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get unmatching checksum values for the pdf pages"""

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)        

        # create sql for unmatching checksum at page level 
        base_release_unmatching_checksum = f""" SELECT T1.FILE_INDEX, 
                                                       T1.FILE_NAME,
                                                       T1.FILE_NAME || '|' || T2.FILE_NAME AS CONCATE_NAME,
                                                       T1.PAGE_NUMBER, 
                                                       T1.PAGE_CHECKSUM, 
                                                       T2.PAGE_CHECKSUM,
                                                       "" AS REMARKS
                                                FROM MAIN.{base_table} AS T1 
                                                INNER JOIN {attach_db_name}.{release_table} AS T2
                                                ON T1.{db_key} = T2.{db_key}
                                                AND T1.PAGE_NUMBER = T2.PAGE_NUMBER
                                                AND T1.PAGE_CHECKSUM != T2.PAGE_CHECKSUM
                                                ; """

        # common base and release metadata 
        cursor.execute(base_release_unmatching_checksum)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    
    
    def get_page_text(self, attach_db:tuple=(), db_key:str="", key_field:str="", page_number_str:str="", base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get text at page level from the base and release database """

        connection = self.get_connection()
        cursor = connection.cursor()
        
        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)    

        # create data extract text
        data_text_sql = f""" SELECT T1.FILE_NAME, T1.PAGE_TEXT_CONTENT, T2.FILE_NAME, T2.PAGE_TEXT_CONTENT
                               FROM MAIN.{base_table.upper()} AS T1
                               INNER JOIN {attach_db_name}.{release_table} AS T2
                               ON T1.{db_key.upper()} = T2.{db_key.upper()}
                               AND T1.PAGE_NUMBER = '{page_number_str}'
                               AND T1.PAGE_NUMBER = T2.PAGE_NUMBER
                               """ 
        if db_key.upper() == "FILE_INDEX":
            data_text_sql = data_text_sql + f" AND T1.{db_key.upper()} = {key_field} "
        else:
            data_text_sql = data_text_sql + f" AND T1.{db_key.upper()} = '{key_field}' "



        # common base and release metadata 
        cursor.execute(data_text_sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows


class JsonCompareDatabase(TravisDatabase):

    def __init__(self, dbname) -> None:
        super().__init__(dbname)      


    def create_table(self, table_name) -> None:
        """ create pdf base and release tables """
        
        connection = self.get_connection()
        cursor = connection.cursor()

        # drop and create if exists 
        cursor.execute(f""" DROP TABLE IF EXISTS {table_name.upper()}; """ )
        cursor.execute(f""" CREATE TABLE {table_name.upper()} (
                                    FILE_INDEX                          INTEGER,
                                    FILE_PATH                           TEXT,                       
                                    FILE_NAME                           TEXT,
                                    KEY_ID                              TEXT,
                                    JSON_DATA                           JSON,
                                    JSON_POSITION                       INTEGER); """)
        
        # Create indexes on this table 
        cursor.execute(f""" CREATE INDEX FILE_INDEX_INDX ON {table_name.upper()} (FILE_INDEX); """)
        cursor.execute(f""" CREATE INDEX FILE_NAME_INDX ON {table_name.upper()} (FILE_NAME); """)
        cursor.execute(f""" CREATE INDEX KEY_ID_INDX ON {table_name.upper()} (KEY_ID); """)

        # Issue commit and close the connection 
        connection.commit()
        cursor.close()


    def get_base_key_id_not_in_release(self, attach_db:tuple=(), base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get base key id not in release file """         

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)        

        # declare sql to extract key id not found in release file
        sql = f""" SELECT A.KEY_ID, "", "", "", "Key Data Not Found in Release File" 
                    FROM (
                            SELECT T1.KEY_ID FROM MAIN.{base_table} AS T1
                                        EXCEPT 
                            SELECT T2.KEY_ID FROM {attach_db_name}.{release_table} AS T2
                        ) AS A """        

        # common base and release metadata 
        cursor.execute(sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    

    def get_release_key_id_not_in_base(self, attach_db:tuple=(), base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get base key id not in release file """         

        connection = self.get_connection()
        cursor = connection.cursor()

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)        

        # declare sql to extract key id not found in release file
        sql = f""" SELECT A.KEY_ID, "", "", "", "Key Data Not Found in Base File" 
                    FROM (
                            SELECT T1.KEY_ID FROM {attach_db_name}.{release_table} AS T1
                                        EXCEPT
                            SELECT T2.KEY_ID FROM MAIN.{base_table} AS T2
                        ) AS A """

        # common base and release metadata 
        cursor.execute(sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows    


    def get_key_count(self, table_name="BASE_TABLE") -> None:
        """ get key count from table """

        connection = self.get_connection()
        cursor = connection.cursor()

        # create attach sql
        sql = f""" SELECT COUNT(KEY_ID) 
                    FROM MAIN.{table_name}; """
        cursor.execute(sql)

        # fetch all records 
        rows = cursor.fetchall()

        connection.commit()
        cursor.close()

        return rows   
    

    def get_common_key_count(self, attach_db:tuple=(), base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", attach_db_name:str="RELEASE_DB") -> tuple:
        """ get common key count """

        connection = self.get_connection()
        cursor = connection.cursor()        

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)        

        # create sql statement for common key count
        sql = f""" SELECT COUNT(T1.KEY_ID) 
                     FROM MAIN.{base_table} AS T1
                    INNER JOIN {attach_db_name}.{release_table}  AS T2
                       ON T1.KEY_ID = T2.KEY_ID; """

        # common base and release metadata 
        cursor.execute(sql)

        # fetch all records 
        rows = cursor.fetchall()

        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows    
    

    def get_attach_connection_cursor(self, attach_db:tuple=(), attach_db_name:str="RELEASE_DB") -> tuple:
        """ return cursor for loop """

        connection = self.get_connection()
        cursor = connection.cursor()        

        # attach the database 
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)               

        return connection, cursor 
    

    def detach_disconnect(self, connection, cursor, attach_db_name) -> None:
        """ detach from the attached db and disconnect """
        
        # detach attached datbase, commit and close the connection 
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        connection.close()
    

    def get_compare_data(self, connection:sqlite3.Connection, cursor:sqlite3.Cursor, base_table:str="BASE_TABLE", release_table:str="RELEASE_TABLE", limit:int=0, offset:int=0, key_id:str="", attach_db_name:str="RELEASE_DB") -> tuple:
        """ extract data in batches for comparision """   

        # create attach sql
        sql = f""" SELECT T1.KEY_ID, 
                          T1.JSON_DATA, 
                          T2.JSON_DATA
                     FROM MAIN.{base_table} AS T1
                    INNER JOIN {attach_db_name}.{release_table} AS T2
                       ON T1.KEY_ID = T2.KEY_ID 
                    ORDER BY T1.KEY_ID
                    LIMIT {limit} OFFSET {offset}; """        

        # get all the records 
        rows = cursor.fetchall()

        # commit thge connection 
        connection.commit()

        # check if rows present 
        if isinstance(rows, list) and len(rows) > 0:
            return rows, rows[-1][0]

        return [], ""