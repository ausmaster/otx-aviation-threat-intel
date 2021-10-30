import math
import types
import sqlite3
import pickle
import os

from alive_progress import alive_bar

from sqlite3 import Error as SQLError
from datetime import date, datetime
from datetime import timedelta

from OTXv2 import OTXv2

pickle.DEFAULT_PROTOCOL = 5
rootDir = os.getcwd()
firstInitializationAllPulses = False
firstInitializationRelPulses = False


def sanitize_description(description: str) -> str:
    description = description.replace("'", "")
    description = description.replace('"', "")
    return description


def convert_seconds(seconds):
    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)
    return hour, min, sec


def convert_days(days):
    years = math.floor(days / 365)
    weeks = math.floor((days - (years * 365))/ 7)
    days = math.floor(days - ((years * 365) + (weeks * 7)))
    return years, weeks, days


class OTXHandler:
    """
    Handles the OTX object from AlienVault
    """
    relevantPulses = []

    def __init__(self, otx_key: str):
        self.relevantPulses = []
        self.typeOfPulses = None
        self.otxObj = OTXv2(api_key = otx_key)

    def __str__(self):
        returnStr = f"//==><==OTX Handler Object==><==\\\\\n//==>Number of Pulses in List: " \
                    f"{len(self.relevantPulses)}<==\\\\\n"
        for pulse in self.relevantPulses:
            returnStr += f"=[{pulse.get('name')} ({pulse.get('id')}) - {pulse.get('author_name')}]=\n"
            returnStr += f"{pulse.get('description')}\n\tAffects Industries:\n"
            for industry in pulse.get('industries'):
                returnStr += f"\t\t{industry}"
            returnStr += "\n\tReferences:\n"
            for reference in pulse.get('references'):
                returnStr += f"\t\t{reference}\n"
            returnStr += "\n"
        return returnStr

    def _get_lastnumdays_pulses(self, days: int = None) -> list:
        """
        Returns list of the all pulses within the last X days.
        :param days: Maximum number of days ago you want pulses to be, defaults to last 30 days
        :return: list of OTX Pulses
        """

        if days:
            # Get current time, timedelta of 30 days, and get the datetime object - 30 days from today
            dateObj = date.today()
            timeD = timedelta(days = days)
            dateObj = dateObj - timeD
            # Return List of all OTX Pulses in last number days
            return self.otxObj.getall(modified_since = dateObj, limit = 100, max_page = 1)
        else:
            return self.otxObj.getall()

    def _get_lastnumdays_pulses_gen(self, days: int = None) -> types.GeneratorType:
        """
        Returns a Generator object that gets all pulses within the last X days.
        :param days: Maximum number of days ago you want pulses to be, defaults to last 30 days
        :return: OTX Pulse generator object
        """
        if days:
            # Get current time, timedelta of 30 days, and get the datetime object - 30 days from today
            dateObj = date.today()
            timeD = timedelta(days = days)
            dateObj = dateObj - timeD
            # Return OTX Pulse Generator
            return self.otxObj.getall_iter(modified_since = dateObj, limit = 100, max_page = 1)
        else:
            # Return OTX Pulse Generator
            return self.otxObj.getall_iter()

    def updatelist_relevantpulses(self, last_numdays: int = None) -> None:
        """
        Updates object variable list, relevantPulses, with Aerospace related pulses from AlienVault
        :param last_numdays: Maximum number of days ago you want pulses to be, defaults to last 30 days
        :return: None
        """
        # Make sure list is clear
        self.relevantPulses.clear()

        if last_numdays:
            with alive_bar(force_tty = True) as bar:
                # For each Pulse created in the last 30 days
                for pulse in self._get_lastnumdays_pulses_gen(days = last_numdays):
                    # Get the industries list, if one of them is "Aerospace", add to relevantPulses list
                    if pulse.get("industries").count("Aerospace") > 0:
                        # Make sure to sanitize description
                        updatedDescription = sanitize_description(pulse.get("description"))
                        pulse["description"] = updatedDescription
                        self.relevantPulses.append(pulse)
                        bar()
        else:
            with alive_bar(force_tty = True) as bar:
                for pulse in self._get_lastnumdays_pulses_gen():
                    # Get the industries list, if one of them is "Aerospace", add to relevantPulses list
                    if pulse.get("industries").count("Aerospace") > 0:
                        # Make sure to sanitize description
                        updatedDescription = sanitize_description(pulse.get("description"))
                        pulse["description"] = updatedDescription
                        self.relevantPulses.append(pulse)
                        bar()

        self.typeOfPulses = "relevant"

    def updatelist_allpulses(self, last_numdays: int = None) -> None:
        """
        Updates object variable list, relevantPulses, with all pulses from AlienVault
        :param last_numdays: Maximum number of days ago you want pulses to be, defaults to last 30 days
        :return: None
        """
        # Make sure list is clear
        self.relevantPulses.clear()

        if last_numdays:
            with alive_bar(force_tty = True) as bar:
                # For each Pulse created in the last number of days
                for pulse in self._get_lastnumdays_pulses_gen(days = last_numdays):
                    # Make sure to sanitize description
                    updatedDescription = sanitize_description(pulse.get("description"))
                    pulse["description"] = updatedDescription
                    self.relevantPulses.append(pulse)
                    bar()
        else:
            with alive_bar(force_tty = True) as bar:
                # For each Pulse created ever since AV was created
                for pulse in self._get_lastnumdays_pulses_gen():
                    # Make sure to sanitize description
                    updatedDescription = sanitize_description(pulse.get("description"))
                    pulse["description"] = updatedDescription
                    self.relevantPulses.append(pulse)
                    bar()

        self.typeOfPulses = "all"

    def sort_pulses(self):
        def key_sort(entry):
            try:
                return datetime.strptime(entry.get("created"), "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError as e:
                print(f"WARNING: Data {entry.get('created')} has violated format. Alleviating.")
                if "does not match format" in str(e):
                    problemString = entry.get("created")
                    problemString += ".000000"
                    return datetime.strptime(problemString, "%Y-%m-%dT%H:%M:%S.%f")
        self.relevantPulses.sort(key = key_sort)


def integrity_pulses_insert(cursor: sqlite3.Cursor, table: str, pulselist: list):
    for pulse in pulselist:
        try:
            cursor.execute(f"""INSERT INTO {table} VALUES {pulse}""")
        except sqlite3.IntegrityError as e:
            if e == "datatype mismatch":
                print("CRITICAL ERROR: Datatype Mismatched! Check Code.")
                return
            pulseId, *_ = pulse
            print(f"WARNING: Data - {pulseId} - already exists in table")
            continue
        except SQLError as e:
            pulseId, _, _, name, description, author_name = pulse
            print(f"ERROR: {e} has occurred! Data: {pulseId}, {name}, {description}, {author_name}")
            continue


def integrity_references_insert(cursor: sqlite3.Cursor, referencelist: list):
    for entry in referencelist:
        pulseId, refs = entry
        for ref in refs:
            try:
                cursor.execute(f"""INSERT INTO reference (pulse_id, reference) VALUES ("{pulseId}", "{ref}")""")
            except sqlite3.IntegrityError:
                print(f"WARNING: Data - {pulseId} - already exists in table")
                continue


class SQLiteDBHandler:
    # Root Directory
    global rootDir

    def __init__(self):
        global firstInitializationAllPulses, firstInitializationRelPulses
        # SQLite variables
        self.pulseList = []
        self.typeOfPulses = None
        self.references = []

        self.allPulsesMeta = AllPulsesMetadata()
        self.relevantPulsesMeta = RelevantPulsesMetadata()

        # Check for Metadata Files
        allPulseMetadata = rootDir + "\\sqlite\\allpulsemeta.p"
        relPulseMetadata = rootDir + "\\sqlite\\relpulsemeta.p"
        print("Checking SQLite Directory and Files -")
        if not os.path.exists(rootDir + "\\sqlite"):
            print("\tSqlite Directory does not exist, creating ... ", end = "")
            os.mkdir(rootDir + "\\sqlite")
            print("Success")
        if not os.path.exists(allPulseMetadata):
            print("\tAll Pulses Metadata File does not exist, creating ... ", end = "")
            temp = open(allPulseMetadata, "x")
            temp.close()
            print("Success")
        if not os.path.exists(relPulseMetadata):
            print("\tRelevant Pulses Metadata File does not exist, creating ... ", end = "")
            temp = open(relPulseMetadata, "x")
            temp.close()
            print("Success")
        self.allPulsesMetaFile = allPulseMetadata
        self.relPulsesMetaFile = relPulseMetadata
        print("Status: OK")

        # Check for Initial DB Connection
        if not self._init_pulsesdb_connect():
            print("Cannot connect to local SQLite DB.")
            raise Exception("Cannot Connect to Relevant_Pulses Database, ensure .db file is in correct directory.")
        print("SQLite DB Connected. Status: OK")
        self.currentCursor = self.currentConnection.cursor()
        print("Connection Cursor Set. Status: OK")

        # Check for pulses table in DB
        if not self._check_table_exists("allpulses"):
            print("All Pulses table does not exist. Initializing table.")
            self._init_pulse_table("allpulses")
            firstInitializationAllPulses = True
        print("All Pulses table exists. Status: OK")
        if not self._check_table_exists("relevantpulses"):
            print("Relevant Pulses table does not exist. Initializing table.")
            self._init_pulse_table("relevantpulses")
            firstInitializationRelPulses = True
        print("Relevant Pulses table exists. Status: OK")
        if not self._check_table_exists("reference"):
            print("Reference table does not exist. Initializing table.")
            self._init_reference_table()
        print("Reference table exists. Status: OK")

    def _check_table_exists(self, table: str) -> bool:
        cursor = self.currentCursor

        # Check if pulses table exists
        cursor.execute(f"""SELECT count(name) FROM sqlite_master WHERE type= 'table' AND name= '{table}'""")
        result = cursor.fetchone()[0]

        if result == 1:
            return True
        elif result == 0:
            return False
        else:
            raise Exception(f"Checking for the existance of {table} produced not a 0 or 1 result for count")

    def _init_pulsesdb_connect(self) -> bool:
        """
        Initial Database connection when SQLiteDBHandler object is created.
        :return: Boolean if Database successfully connected
        """

        path = rootDir + "\\sqlite"
        # Ensure sqlite directory exists
        if not os.path.exists(path):
            try:
                os.mkdir(path)
            except OSError:
                print("Failed to create directory for sqlite DB. Functionality Disabled.")
                return False

        # Attempt to connect to SQL Database
        try:
            relPulsesDBConnect = sqlite3.connect(path + "\\Relevant_Pulses.sqlite3")
        except SQLError as e:
            print(f"Failed to connect to SQLite DB.\nError: {e}")
            return False
        else:
            self.currentConnection = relPulsesDBConnect
            return True

    def _init_pulse_table(self, table: str) -> None:
        """
        Initial query into database, handles first connection into DB. Creates selected table if does not exist.
        :return: None
        """
        cursor = self.currentCursor

        if not self._check_table_exists(table):
            print(f"Table does not exist. Creating {table} table.")
            cursor.execute(f"""CREATE TABLE {table} (pulse_id TEXT PRIMARY KEY NOT NULL, name TEXT, created TEXT, 
                    modified TEXT, description TEXT, author TEXT)""")
            self.currentConnection.commit()
            # print("Created table. Returning True.")

    def _init_reference_table(self):
        cursor = self.currentCursor

        #if result == 1:
        # print("Table Exists, returning True")
        if not self._check_table_exists("reference"):
            print("Table does not exist. Creating references table.")
            cursor.execute("""CREATE TABLE reference (pulse_id text NOT NULL, reference text)""")
            self.currentConnection.commit()
            # print("Created table. Returning True.")

    def digest_pulses(self, otx_object: OTXHandler) -> None:
        """
        Digests the pulses from OTX object into an SQL Insertable form to insert into a pulse table.
        :param otx_object: OTXHandler object that contains all relevantPulses
        :return: None
        """
        # Make sure pulseList and references is reset upon digest
        self.pulseList.clear()
        self.references.clear()

        for pulse in otx_object.relevantPulses:
            pulseId = pulse.get("id")
            name = pulse.get("name")
            created = pulse.get("created")
            modified = pulse.get("modified")
            description = pulse.get("description")
            author = pulse.get("author_name")
            references = pulse.get("references")
            self.pulseList.append((pulseId, name, created, modified, description, author))
            self.references.append((pulseId, references))
        self.typeOfPulses = otx_object.typeOfPulses

    def insert_pulses(self, table: str) -> None:
        """
        Inserts pulses into the selected SQL table.
        :param table: string of the table to insert pulses into, has to be either 'allpulses' or 'relevantpulses'
        :return: None
        """
        if len(self.pulseList) <= 0:
            return
        if not self._check_table_exists(table):
            print("Table does not exist, returning")
            return

        try:
            self.currentCursor.executemany(f"""INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)""", self.pulseList)
            self.currentConnection.commit()
        except sqlite3.IntegrityError:
            print("WARNING: Data in Pulse List violates Integrity. Manually running SQL Statements on each piece of "
                  "data.")
            integrity_pulses_insert(self.currentCursor, table, self.pulseList)
            self.currentConnection.commit()

    def insert_references(self):
        if len(self.references) <= 0:
            return

        if not self._check_table_exists("reference"):
            print("Reference Table does not exist, returning")
            return

        integrity_references_insert(self.currentCursor, self.references)
        self.currentConnection.commit()

    def insert_everything(self):
        if self.typeOfPulses == "relevant":
            self.insert_pulses("relevantpulses")
        else:
            self.insert_pulses("allpulses")
        self.insert_references()

    def purge_table(self, table: str) -> None:
        """
        Purges a table in the database specified by the string, either "allpulses", "relevantpulses", or "reference".
        :param table: String of the table to delete
        :return: None
        """
        try:
            self.currentCursor.execute(f"""DELETE FROM {table}""")
            self.currentConnection.commit()
            self.currentCursor.execute(f"""DROP TABLE {table}""")
            self.currentConnection.commit()
        except sqlite3.OperationalError:
            return

    def purge_alltables(self) -> None:
        """
        Purges all the tables in the pulses DB.

        :return: None
        """
        self.purge_table("allpulses")
        self.purge_table("relevantpulses")
        self.purge_table("reference")

    def reset_table(self, table: str) -> None:
        """
        :param table: Table to reset
        :return: None
        """
        if table in ["allpulses", "relevantpulses"]:
            self.purge_table(table)
            self._init_pulse_table(table)
        elif table == "reference":
            self.purge_table(table)
            self._init_reference_table()


class AllPulsesMetadata:
    lastUpdated: datetime


class RelevantPulsesMetadata:
    lastUpdated: datetime


class ApplicationDirector:
    def __init__(self, otx_key: str):
        self.otxHandler = OTXHandler(otx_key)
        self.dbHandler = SQLiteDBHandler()
        self.currentCursor = self.dbHandler.currentCursor
        self.apMeta = self.dbHandler.allPulsesMeta
        self.rpMeta = self.dbHandler.relevantPulsesMeta
        self.apMetaFile = self.dbHandler.allPulsesMetaFile
        self.rpMetaFile = self.dbHandler.relPulsesMetaFile

    def reset_allpulses(self, days: int = None):
        print("Initializing All Pulses")
        print("Purging All Pulse table if exists")
        self.dbHandler.purge_table("allpulses")
        print("Purge Complete")
        print("Obtaining Pulses")
        self.otxHandler.updatelist_allpulses(last_numdays = days)
        print("Pulses Obtained, Digesting Pulses")
        self.dbHandler.digest_pulses(self.otxHandler)
        print("Digest Complete")
        print("Incerting Pulses")
        self.dbHandler.insert_everything()
        print("Incerting Complete")
        print("Dumping current time into All Pulses Metadata file")
        self.apMeta.lastUpdated = datetime.today()
        with open(self.apMetaFile, 'wb') as f:
            pickle.dump(self.apMeta, file = f)
            print("Metadata dumping complete")

    def reset_relevantpulses(self, days: int = None):
        self.dbHandler.purge_table("relevantpulses")
        self.otxHandler.updatelist_relevantpulses(last_numdays = days)
        self.dbHandler.digest_pulses(self.otxHandler)
        self.dbHandler.insert_everything()
        print("Dumping current time into Relevant Pulses Metadata file")
        self.rpMeta.lastUpdated = datetime.today()
        with open(self.rpMetaFile, 'wb') as f:
            pickle.dump(self.rpMeta, file = f)
            print("Metadata dumping complete")

    def check_for_initialization(self, table: str) -> bool:
        global firstInitializationAllPulses, firstInitializationRelPulses
        if firstInitializationAllPulses and table == "allpulses":
            print("All Pulses table has not been initially populated. Populating All Pulses table with current data. "
                  "This may take a while. Please wait...")
            self.otxHandler.updatelist_allpulses()
            self.dbHandler.digest_pulses(self.otxHandler)
            self.dbHandler.insert_everything()
            firstInitializationAllPulses = False
            return True

        if firstInitializationRelPulses and table == "relevantpulses":
            print("Relevant Pulses table has not been initially populated. Populating Relevant Pulses table with "
                  "current data. This may take a while. Please wait...")
            self.otxHandler.updatelist_relevantpulses()
            self.dbHandler.digest_pulses(self.otxHandler)
            self.dbHandler.insert_everything()
            firstInitializationRelPulses = False
            return True

        return False

    def update_alltables(self) -> None:
        """
        Updates all the current pulse tables in the DB which is "allpulses" and "relevantpulses".
        :return: None
        """
        self._update_table("allpulses")
        self._update_table("relevantpulses")

    def _update_table(self, table: str) -> None:
        """
        Updates a table in the DB with the current data from the feed in OTX. Will additionally log metadata to the
        associated metadata file to keep track of last time the table was updated.
        :param table: Table to update
        :return: None
        """

        # Check to make sure correct input was passed
        if table != "allpulses" and table != "relevantpulses":
            print("Invalid Table. Select either allpulses or relevantpulses.")
            return

        # Set the metaFile and meta object variables to use later
        if table == "allpulses":
            allPulsesMode = True
            metadataFile = self.apMetaFile
            meta = self.apMeta
        else:
            allPulsesMode = False
            metadataFile = self.rpMetaFile
            meta = self.rpMeta

        # Setup the current metafile to have lastUpdated be set to right now
        meta.lastUpdated = datetime.today()

        # If the table was *just* made, go ahead and populate the table with everything from OTX
        if self.check_for_initialization(table):
            print("Table Populated. Skipping to Writing Metadata to file.")
            self._write_metafile(meta, metadataFile)
            return

        # Read the metadata file, make sure we can read it
        if not (lastDBInfo := self._read_metafile(metadataFile)):
            print("!ERROR READING METADATA FILE!")
            return

        # Check for lastUpdated in Metadata file, it *should* be set to a date
        if allPulsesMode and lastDBInfo.lastUpdated is None:
            print("Make sure to run init_insert_Xpulses first before proceeding")
            return
        elif not allPulsesMode and lastDBInfo.lastUpdated is None:
            print("Make sure to run init_insert_Xpulses first before proceeding")
            return

        # Get the time delta between current time and last time it was updated
        diff = meta.lastUpdated - lastDBInfo.lastUpdated
        # For easier reading with the print, to tell the user how long ago it was updated
        hours, mins, secs = convert_seconds(diff.seconds)
        years, weeks, days = convert_days(diff.days)
        # Print last time it was updated
        print(f"Last {table} Pulse Table Update: {lastDBInfo.lastUpdated}\nTime Since Last Update: {days} Days,"
              f" {weeks} Weeks, {years} Years: {hours} Hours, {mins} Mins, {secs} Seconds Ago")
        # For the case that diff.days does not exist (e.g. if its been less than a day), defaults to 1 day if there has
        # not been a day since the last update
        daysSince = diff.days if diff.days else 1

        # According to which table to update, get the pulses since the last update
        if allPulsesMode:
            self.otxHandler.updatelist_allpulses(last_numdays = daysSince)
        else:
            self.otxHandler.updatelist_relevantpulses(last_numdays = daysSince)

        # Start to do SQL stuff, get the current cursor, get all data currently in the table
        cursor = self.dbHandler.currentCursor
        cursor.execute(f"""SELECT * FROM {table} ORDER BY created, modified ASC""")
        dbPulses = cursor.fetchall()

        print(f"Fetched Pulses from {table} Table")

        # Sort the pulses by it's creation date to make the comparison process more faster
        self.otxHandler.sort_pulses()
        print(f"Iterating through Pulses in {table} Table")
        with alive_bar(total = len(dbPulses), bar = "smooth", force_tty = True) as bar:
            for dbPulse in dbPulses:
                pulseId, *_ = dbPulse
                relivantPulseCopy = self.otxHandler.relevantPulses.copy()
                for index, pulse in enumerate(relivantPulseCopy):
                    if pulse.get("id") == pulseId:
                        del self.otxHandler.relevantPulses[index]
                        break
                bar()
        print("Done Iterating through current Pulses.")
        if len(self.otxHandler.relevantPulses) != 0:
            def print_pulsedata(pulses: list):
                for pulse in pulses:
                    print(f"\t{pulse.get('name')} ({pulse.get('id')})")
            print(f"Obtained {len(self.otxHandler.relevantPulses)} Pulses to Insert:")
            print_pulsedata(self.otxHandler.relevantPulses)
            self.dbHandler.digest_pulses(self.otxHandler)
            print("Inserting Pulses and References...", end = " ")
            self.dbHandler.insert_everything()
            print("Successful")
        else:
            print("No Pulses to Insert.")

        self._write_metafile(meta, metadataFile)
        print("Update Done!")

    @staticmethod
    def _read_metafile(meta_file: str):
        print("Reading Metadata file.")
        try:
            with open(meta_file, "rb") as f:
                lastDBInfo = pickle.load(f)
        except EOFError:
            print("Error. No Data to Read.")
            return None
        except FileNotFoundError:
            print("Error. Metadata file not found.")
            return None

        print("Reading Complete.")
        return lastDBInfo

    @staticmethod
    def _write_metafile(meta_obj, meta_file: str) -> None:
        print("Dumping Metadata to File")
        with open(meta_file, 'wb') as f:
            pickle.dump(meta_obj, file = f)
        print("Metadata dumping complete")
