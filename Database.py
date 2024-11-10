from DBConn import DatabaseConnection
from Model import Model
import json
from os import getenv


class Template:
    """Helper class to create template objects"""

    def __init__(self):
        self.database = None
        self.tables = None

    @staticmethod
    def loadFromFile(file_path):
        if not file_path.split(".")[-1] == "json":
            raise ValueError("Template file extension must be .json")

        t = Template()
        with open(file_path, 'r') as f:
            data = json.load(f)
        for k in data['database']:
            if data['database'][k] == "ENV":
                data['database'][k] = getenv(data['database'][k], None)

        t.__dict__.update(data)
        return t

    def saveToFile(self, file_path):
        with open(file_path, 'w') as template_file:
            json.dump(self.__dict__, template_file, indent=4)

    @staticmethod
    def generateEmptyTemplate():
        t = Template()
        t.has_been_created = False
        t.database = {
            "name": "DB_NAME",
            "host": "HOST",
            "port": "PORT",
            "username": "ENV",
            "password": "ENV"
        }
        t.tables = list()
        return t


class Database:
    """
    Base class that put in relation DatabaseConnection() and Model()
    User should define a child class of Database() in order to abstract even more the table management
    """
    def __init__(self):
        self.connection = None

        self.template = None

        self.models_names = []

        self.name = None
        self.host = None
        self.port = None
        self.user = None

    @staticmethod
    def loadFromTemplate(path):
        db = Database()
        db.template = Template.loadFromFile(path)

        db.name = db.template.database['name']
        db.host = db.template.database['host']
        db.port = db.template.database['port']
        db.user = db.template.database['username']
        db.password = db.template.database['password']

        db.connection = DatabaseConnection(db.name, db.user, db.password, host=db.host, port=db.port)

        for models in db.template.tables:
            d = {models["name"]: Model(db.connection, models)}
            db.__dict__.update(d)
            db.models_names.append(models["name"])

        return db

    def _check(self) -> None:
        if self.template is None:
            raise ValueError("Template has not been created")
        if len(self.models_names) == 0:
            raise ValueError("Models have not been created")

    def createModels(self) -> None:
        """
        Create all models in the database
        """
        self._check()

        for k in self.models_names:
            getattr(self, k).createTable()

    def dropModels(self) -> None:
        """
        Drop all models in the database
        """
        self._check()

        for k in self.models_names:
            getattr(self, k).dropTable()

    def resetModels(self) -> None:
        """
        Reset all models in the database by dropping all tables and then creating new tables
        """
        self._check()

        self.dropModels()
        self.createModels()

    @staticmethod
    def generateEmptyDatabase():
        db = Database()
        db.template = Template.generateEmptyTemplate()

        db.name = db.template.database['name']
        db.host = db.template.database['host']
        db.port = db.template.database['port']
        db.user = db.template.database['username']
        db.password = db.template.database['password']

        db.connection = DatabaseConnection(db.name, db.user, db.password, host=db.host, port=db.port)

        return db


if __name__ == "__main__":
    db_test = Database.loadFromTemplate("test_template.json")
    db_test.resetModels()

    db_test.test_table.insertRow(name="Alice", age=0)
