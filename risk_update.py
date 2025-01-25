import tkinter as tk
import xml.etree.ElementTree as ET
from tkinter import messagebox, ttk
from datetime import datetime
import requests
import ssl
from urllib3 import poolmanager
from requests.adapters import HTTPAdapter
import os
import time
import logging
import sqlalchemy as sq

logging.basicConfig(filename=f"aml_risk_update_{datetime.now().strftime('%d_%m_%Y_%H_%M')}.log", level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.options |= ssl.OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)


class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect_to_database(self, server=None, database=None, user=None, password=None, driver="ODBC Driver 17 for SQL Server", port="1433", max_attempts=3):
        if not all([server, database, user, password]):
            raise ValueError("Database connection details are incomplete")

        for i in range(max_attempts):
            try:
                connection_string = (
                    f"DRIVER={{{driver}}};SERVER={server},{port};DATABASE={database};UID={user};PWD={password}"
                )
                connection_url = sq.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
                engine = sq.create_engine(connection_url)
                self.conn = engine.raw_connection()
                self.cursor = self.conn.cursor()
                return
            except Exception as e:
                logging.error(f"Database connection attempt {i + 1} failed: {e}")
                time.sleep(5)
        raise Exception("Failed to connect to the database after maximum attempts")

    def check_connection(self):
        if not self.conn or not self.cursor:
            raise Exception("No active database connection to check")
        try:
            self.cursor.execute("SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES")
        except Exception as e:
            logging.error(f"Connection check failed: {e}")
            raise e

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")
        finally:
            self.conn = None
            self.cursor = None

def get_text(element, tag):
    el = element.find(tag)
    return el.text if el is not None else None


def insert_and_get_id(db_manager, table, columns, values):
    if all(v is None for v in values):
        select_sql = f"SELECT ID FROM {table} WHERE {columns[0]} IS NULL"
        db_manager.cursor.execute(select_sql)
        result = db_manager.cursor.fetchone()
        if result:
            return result[0]
        else:
            placeholders = ', '.join('?' * len(values))
            columns_str = ', '.join(columns)
            insert_sql = f"INSERT INTO {table} ({columns_str}) OUTPUT Inserted.ID VALUES ({placeholders})"
            db_manager.cursor.execute(insert_sql, values)
            return db_manager.cursor.fetchone()[0]

    conditions = ' AND '.join([f"{column} = ?" for column in columns])
    select_sql = f"SELECT ID FROM {table} WHERE {conditions}"
    db_manager.cursor.execute(select_sql, values)
    result = db_manager.cursor.fetchone()
    if result:
        return result[0]
    else:
        placeholders = ', '.join('?' * len(values))
        columns_str = ', '.join(columns)
        insert_sql = f"INSERT INTO {table} ({columns_str}) OUTPUT Inserted.ID VALUES ({placeholders})"
        db_manager.cursor.execute(insert_sql, values)
        return db_manager.cursor.fetchone()[0]


def insert_individual(db_manager, individual):
    dataid = get_text(individual, 'DATAID')
    versionnum = get_text(individual, 'VERSIONNUM')
    first_name = get_text(individual, 'FIRST_NAME')
    second_name = get_text(individual, 'SECOND_NAME')
    third_name = get_text(individual, 'THIRD_NAME')
    un_list_type = get_text(individual, 'UN_LIST_TYPE')
    reference_number = get_text(individual, 'REFERENCE_NUMBER')
    listed_on = get_text(individual, 'LISTED_ON')
    name_original_script = get_text(individual, 'NAME_ORIGINAL_SCRIPT')
    comments1 = get_text(individual, 'COMMENTS1')
    sort_key = get_text(individual, 'SORT_KEY')
    sort_key_last_mod = get_text(individual, 'SORT_KEY_LAST_MOD')
    gender = get_text(individual, 'GENDER')
    fourth_name = get_text(individual, 'FOURTH_NAME')

    title_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedTitle', ['VALUE'], [get_text(individual, 'TITLE/VALUE')])
    designation_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedDesignation', ['VALUE'],
                                       [get_text(individual, 'DESIGNATION/VALUE')])
    nationality_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedNationality', ['VALUE'],
                                       [get_text(individual, 'NATIONALITY/VALUE')])
    list_type_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedListType', ['VALUE'],
                                     [get_text(individual, 'LIST_TYPE/VALUE')])
    last_day_updated_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedLastDayUpdated', ['VALUE'],
                                            [get_text(individual, 'LAST_DAY_UPDATED/VALUE')])

    alias_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividualAlias', [
        'QUALITY', 'ALIAS_NAME', 'DATE_OF_BIRTH', 'NOTE', 'CITY_OF_BIRTH', 'COUNTRY_OF_BIRTH'
    ], [
                                     get_text(individual, 'INDIVIDUAL_ALIAS/QUALITY'),
                                     get_text(individual, 'INDIVIDUAL_ALIAS/ALIAS_NAME'),
                                     get_text(individual, 'INDIVIDUAL_ALIAS/DATE_OF_BIRTH'),
                                     get_text(individual, 'INDIVIDUAL_ALIAS/NOTE'),
                                     get_text(individual, 'INDIVIDUAL_ALIAS/CITY_OF_BIRTH'),
                                     get_text(individual, 'INDIVIDUAL_ALIAS/COUNTRY_OF_BIRTH')
                                 ])

    address_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividualAddress', [
        'COUNTRY', 'STREET', 'CITY', 'STATE_PROVINCE', 'NOTE', 'ZIP_CODE'
    ], [
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/COUNTRY'),
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/STREET'),
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/CITY'),
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/STATE_PROVINCE'),
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/NOTE'),
                                       get_text(individual, 'INDIVIDUAL_ADDRESS/ZIP_CODE')
                                   ])

    dob_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividualDateOfBirth', [
        'TYPE_OF_DATE', 'YEAR', 'FROM_YEAR', 'TO_YEAR', 'NOTE', 'DATE'
    ], [
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/TYPE_OF_DATE'),
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/YEAR'),
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/FROM_YEAR'),
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/TO_YEAR'),
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/NOTE'),
                                   get_text(individual, 'INDIVIDUAL_DATE_OF_BIRTH/DATE')
                               ])

    pob_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividualPlaceOfBirth', [
        'CITY', 'STATE_PROVINCE', 'COUNTRY', 'NOTE', 'STREET'
    ], [
                                   get_text(individual, 'INDIVIDUAL_PLACE_OF_BIRTH/CITY'),
                                   get_text(individual, 'INDIVIDUAL_PLACE_OF_BIRTH/STATE_PROVINCE'),
                                   get_text(individual, 'INDIVIDUAL_PLACE_OF_BIRTH/COUNTRY'),
                                   get_text(individual, 'INDIVIDUAL_PLACE_OF_BIRTH/NOTE'),
                                   get_text(individual, 'INDIVIDUAL_PLACE_OF_BIRTH/STREET')
                               ])

    doc_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividualDocument', [
        'TYPE_OF_DOCUMENT', 'TYPE_OF_DOCUMENT2', 'NUMBER', 'COUNTRY_OF_ISSUE', 'NOTE',
        'ISSUING_COUNTRY', 'DATE_OF_ISSUE', 'CITY_OF_ISSUE'
    ], [
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/TYPE_OF_DOCUMENT'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/TYPE_OF_DOCUMENT2'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/NUMBER'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/COUNTRY_OF_ISSUE'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/NOTE'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/ISSUING_COUNTRY'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/DATE_OF_ISSUE'),
                                   get_text(individual, 'INDIVIDUAL_DOCUMENT/CITY_OF_ISSUE')
                               ])

    individual_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedIndividual', [
        'DATAID', 'VERSIONNUM', 'FIRST_NAME', 'SECOND_NAME', 'THIRD_NAME', 'UN_LIST_TYPE',
        'REFERENCE_NUMBER', 'LISTED_ON', 'NAME_ORIGINAL_SCRIPT', 'COMMENTS1', 'Title_ID',
        'Designation_ID', 'Nationality_ID', 'List_type_ID', 'Last_day_updated_ID',
        'Individual_alias_ID', 'Individual_address_ID', 'Individual_date_of_birth_ID',
        'Individual_place_of_birth_ID', 'Individual_document_ID', 'SORT_KEY', 'SORT_KEY_LAST_MOD', 'GENDER',
        'FOURTH_NAME'
    ], [
                                          dataid, versionnum, first_name, second_name, third_name, un_list_type,
                                          reference_number, listed_on, name_original_script, comments1, title_id,
                                          designation_id, nationality_id, list_type_id, last_day_updated_id,
                                          alias_id, address_id, dob_id, pob_id, doc_id,
                                          sort_key, sort_key_last_mod, gender, fourth_name
                                      ])

    return individual_id


def insert_entity(db_manager, entity):
    dataid = get_text(entity, 'DATAID')
    versionnum = get_text(entity, 'VERSIONNUM')
    first_name = get_text(entity, 'FIRST_NAME')
    un_list_type = get_text(entity, 'UN_LIST_TYPE')
    reference_number = get_text(entity, 'REFERENCE_NUMBER')
    listed_on = get_text(entity, 'LISTED_ON')
    comments1 = get_text(entity, 'COMMENTS1')
    sort_key = get_text(entity, 'SORT_KEY')
    sort_key_last_mod = get_text(entity, 'SORT_KEY_LAST_MOD')
    name_original_script = get_text(entity, 'NAME_ORIGINAL_SCRIPT')

    list_type_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedListType', ['VALUE'],
                                     [get_text(entity, 'LIST_TYPE/VALUE')])
    last_day_updated_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedLastDayUpdated', ['VALUE'],
                                            [get_text(entity, 'LAST_DAY_UPDATED/VALUE')])

    alias_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedEntityAlias', [
        'QUALITY', 'ALIAS_NAME', 'NOTE'
    ], [
                                     get_text(entity, 'ENTITY_ALIAS/QUALITY'),
                                     get_text(entity, 'ENTITY_ALIAS/ALIAS_NAME'),
                                     get_text(entity, 'ENTITY_ALIAS/NOTE')
                                 ])

    address_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedEntityAddress', [
        'STREET', 'CITY', 'COUNTRY', 'ZIP_CODE', 'STATE_PROVINCE', 'NOTE'
    ], [
                                       get_text(entity, 'ENTITY_ADDRESS/STREET'),
                                       get_text(entity, 'ENTITY_ADDRESS/CITY'),
                                       get_text(entity, 'ENTITY_ADDRESS/COUNTRY'),
                                       get_text(entity, 'ENTITY_ADDRESS/ZIP_CODE'),
                                       get_text(entity, 'ENTITY_ADDRESS/STATE_PROVINCE'),
                                       get_text(entity, 'ENTITY_ADDRESS/NOTE')
                                   ])

    entity_id = insert_and_get_id(db_manager, 'dbo.ConsolidatedEntity', [
        'DATAID', 'VERSIONNUM', 'FIRST_NAME', 'UN_LIST_TYPE', 'REFERENCE_NUMBER',
        'LISTED_ON', 'COMMENTS1', 'List_type_ID', 'Last_day_updated_ID',
        'Entity_alias_ID', 'Entity_address_ID', 'SORT_KEY', 'SORT_KEY_LAST_MOD', 'NAME_ORIGINAL_SCRIPT'
    ], [
                                      dataid, versionnum, first_name, un_list_type, reference_number,
                                      listed_on, comments1, list_type_id, last_day_updated_id,
                                      alias_id, address_id, sort_key, sort_key_last_mod, name_original_script
                                  ])

    return entity_id


def create_tables(db_manager):
    create_tables_script = """ 

    USE AMLDatabase;

        IF OBJECT_ID('dbo.ConsolidatedTitle', 'U') IS NULL
            BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedTitle (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                VALUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedDesignation', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedDesignation (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                VALUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedNationality', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedNationality (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                VALUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedListType', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedListType (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                VALUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedLastDayUpdated', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedLastDayUpdated (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                VALUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividualAlias', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividualAlias (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                QUALITY VARCHAR(500),
                ALIAS_NAME VARCHAR(500),
                DATE_OF_BIRTH VARCHAR(500),
                NOTE NVARCHAR(max),
                CITY_OF_BIRTH VARCHAR(500),
                COUNTRY_OF_BIRTH VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividualAddress', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividualAddress (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                COUNTRY VARCHAR(500),
                STREET VARCHAR(500),
                CITY VARCHAR(500),
                STATE_PROVINCE VARCHAR(500),
                NOTE NVARCHAR(max),
                ZIP_CODE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividualDateOfBirth', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividualDateOfBirth (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                TYPE_OF_DATE VARCHAR(500),
                YEAR VARCHAR(500),
                FROM_YEAR VARCHAR(500),
                TO_YEAR VARCHAR(500),
                NOTE NVARCHAR(max),
                DATE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividualPlaceOfBirth', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividualPlaceOfBirth (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                CITY VARCHAR(500),
                STATE_PROVINCE VARCHAR(500),
                COUNTRY VARCHAR(500),
                NOTE NVARCHAR(max),
                STREET VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividualDocument', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividualDocument (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                TYPE_OF_DOCUMENT VARCHAR(500),
                TYPE_OF_DOCUMENT2 VARCHAR(500),
                NUMBER VARCHAR(500),
                COUNTRY_OF_ISSUE VARCHAR(500),
                NOTE NVARCHAR(max),
                ISSUING_COUNTRY VARCHAR(500),
                DATE_OF_ISSUE VARCHAR(500),
                CITY_OF_ISSUE VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividual', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividual (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                DATAID VARCHAR(500),
                VERSIONNUM VARCHAR(500),
                FIRST_NAME VARCHAR(500),
                SECOND_NAME VARCHAR(500),
                THIRD_NAME VARCHAR(500),
                FOURTH_NAME VARCHAR(500),
                UN_LIST_TYPE VARCHAR(500),
                REFERENCE_NUMBER VARCHAR(500),
                LISTED_ON VARCHAR(500),
                NAME_ORIGINAL_SCRIPT NVARCHAR(500),
                COMMENTS1 NVARCHAR(max),
                Title_ID INT,
                Designation_ID INT,
                Nationality_ID INT,
                List_type_ID INT,
                Last_day_updated_ID INT,
                Individual_alias_ID INT,
                Individual_address_ID INT,
                Individual_date_of_birth_ID INT,
                Individual_place_of_birth_ID INT,
                Individual_document_ID INT,
                SORT_KEY VARCHAR(500),
                SORT_KEY_LAST_MOD VARCHAR(500),
                GENDER VARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedIndividuals', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedIndividuals (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                Individual_ID INT,
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedEntityAlias', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedEntityAlias (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                QUALITY VARCHAR(500),
                ALIAS_NAME VARCHAR(500),
                NOTE NVARCHAR(max),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedEntityAddress', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedEntityAddress (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                STREET VARCHAR(500),
                CITY VARCHAR(500),
                COUNTRY VARCHAR(500),
                ZIP_CODE VARCHAR(500),
                STATE_PROVINCE VARCHAR(500),
                NOTE NVARCHAR(max),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedEntity', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedEntity (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                DATAID VARCHAR(500),
                VERSIONNUM VARCHAR(500),
                FIRST_NAME VARCHAR(500),
                UN_LIST_TYPE VARCHAR(500),
                REFERENCE_NUMBER VARCHAR(500),
                LISTED_ON VARCHAR(500),
                COMMENTS1 NVARCHAR(max),
                List_type_ID INT,
                Last_day_updated_ID INT,
                Entity_alias_ID INT,
                Entity_address_ID INT,
                SORT_KEY VARCHAR(500),
                SORT_KEY_LAST_MOD VARCHAR(500),
                NAME_ORIGINAL_SCRIPT NVARCHAR(500),
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

        IF OBJECT_ID('dbo.ConsolidatedEntities', 'U') IS NULL
        BEGIN
            CREATE TABLE AMLDatabase.dbo.ConsolidatedEntities (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                Entity_ID INT,
                [Inserted] [datetime] NULL DEFAULT (getdate())
            );
        END;

    """
    for i in range(3):
        try:
            db_manager.cursor.execute(create_tables_script)
            logging.info("Tables Created")
            break
        except Exception as e:
            error_message = 'create_tables() Error:', e
            logging.error(error_message)
    db_manager.conn.commit()


def truncate_tables(db_manager):
    tables_in_order = [
        "AMLDatabase.dbo.ConsolidatedEntities",
        "AMLDatabase.dbo.ConsolidatedIndividuals",
        "AMLDatabase.dbo.ConsolidatedIndividualDocument",
        "AMLDatabase.dbo.ConsolidatedIndividualPlaceOfBirth",
        "AMLDatabase.dbo.ConsolidatedIndividualDateOfBirth",
        "AMLDatabase.dbo.ConsolidatedIndividualAddress",
        "AMLDatabase.dbo.ConsolidatedIndividualAlias",
        "AMLDatabase.dbo.ConsolidatedEntityAlias",
        "AMLDatabase.dbo.ConsolidatedEntityAddress",
        "AMLDatabase.dbo.ConsolidatedIndividual",
        "AMLDatabase.dbo.ConsolidatedEntity",
        "AMLDatabase.dbo.ConsolidatedLastDayUpdated",
        "AMLDatabase.dbo.ConsolidatedListType",
        "AMLDatabase.dbo.ConsolidatedNationality",
        "AMLDatabase.dbo.ConsolidatedDesignation",
        "AMLDatabase.dbo.ConsolidatedTitle"
    ]

    for table in tables_in_order:
        try:
            db_manager.cursor.execute(f"""TRUNCATE TABLE {table}""")
        except Exception as e:
            error_message = 'truncate_tables() Error:', e
            logging.error(error_message)
    db_manager.conn.commit()
    logging.info("All tables truncated.")




def process_data(progress_var, status_label, root, db_details):
    db_manager = DatabaseManager()
    try:
        url = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"

        session = requests.Session()
        adapter = SSLAdapter()
        session.mount('https://', adapter)

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            error_message = "Request timed out. Please try again later."
            messagebox.showerror("Timeout Error", error_message)
            logging.error(error_message)
            return
        except requests.exceptions.RequestException as e:
            error_message = f"Network error occurred: {e}"
            messagebox.showerror("Network Error", error_message)
            logging.error(error_message)
            return
        logging.info(f"Request URL: {url}")
        if response.status_code == 200:
            xml_content = response.content
            xml_root = ET.fromstring(xml_content)

            individuals = xml_root.findall('.//INDIVIDUAL')
            entities = xml_root.findall('.//ENTITY')

            logging.info(f"Found {len(individuals)} individuals and {len(entities)} entities")
        else:
            error_message = '''Could not find any data on individuals or entities on the URL! Program finished unsuccessfully. Please check the URL and try again.'''
            messagebox.showerror("Fail", error_message)
            logging.error(error_message)
            return

        total_items = len(individuals) + len(entities)

        db_manager.connect_to_database(**db_details)
        create_tables(db_manager)
        truncate_tables(db_manager)

        processed_items = 0

        for individual in individuals:
            individual_id = insert_individual(db_manager, individual)
            db_manager.cursor.execute("INSERT INTO dbo.ConsolidatedIndividuals (Individual_ID) VALUES (?)",
                                      individual_id)
            processed_items += 1
            progress = (processed_items / total_items) * 100
            progress_var.set(progress)
            status_label.config(text=f"Processing individuals: {progress:.1f}%")
            root.update_idletasks()

        for entity in entities:
            entity_id = insert_entity(db_manager, entity)
            db_manager.cursor.execute("INSERT INTO dbo.ConsolidatedEntities (Entity_ID) VALUES (?)", entity_id)
            processed_items += 1
            progress = (processed_items / total_items) * 100
            progress_var.set(progress)
            status_label.config(text=f"Processing entities: {progress:.1f}%")
            root.update_idletasks()

        db_manager.conn.commit()
        messagebox.showinfo("Success", "Risk updates completed successfully!")
        logging.info("Risk updating completed successfully!")
    except Exception as e:
        if db_manager.conn:
            db_manager.conn.rollback()
        error_message = f"Error occurred during processing: {e}"
        logging.error(error_message)
        messagebox.showerror("Error", error_message)
    finally:
        if db_manager.conn:
            db_manager.close_connection()


def create_gui():
    def start_process():
        button.config(state=tk.DISABLED)
        db_details = {
            "server": server_entry.get(),
            "database": database_entry.get(),
            "user": user_entry.get(),
            "password": password_entry.get(),
            "driver": "ODBC Driver 17 for SQL Server",
            "port": "1433"
        }
        process_data(progress_var, status_label, window, db_details)
        button.config(state=tk.NORMAL)

    window = tk.Tk()
    window.title("AML Risk Update")
    window.geometry("800x600")

    tk.Label(window, text="Server:").pack(pady=5)
    server_entry = tk.Entry(window, width=50)
    server_entry.pack(pady=5)

    tk.Label(window, text="Database:").pack(pady=5)
    database_entry = tk.Entry(window, width=50)
    database_entry.pack(pady=5)

    tk.Label(window, text="Username:").pack(pady=5)
    user_entry = tk.Entry(window, width=50)
    user_entry.pack(pady=5)

    tk.Label(window, text="Password:").pack(pady=5)
    password_entry = tk.Entry(window, show="*", width=50)
    password_entry.pack(pady=5)

    progress_var = tk.DoubleVar()
    progress_bar = ttk.Progressbar(window, variable=progress_var, maximum=100)
    progress_bar.pack(pady=20)

    status_label = tk.Label(window, text="")
    status_label.pack(pady=10)

    button = tk.Button(window, text="Update AML Risks", command=start_process,
                       font=("Arial", 14), bg="green", fg="white")
    button.pack(pady=50)

    version_label = ttk.Label(window, text="Program Version: AML 1.4")
    version_label.pack(side=tk.RIGHT, anchor=tk.SE)
    window.mainloop()


if __name__ == "__main__":
    create_gui()