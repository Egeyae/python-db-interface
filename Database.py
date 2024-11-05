from DBConn import DatabaseConnection
from Model import Model
import json
from os import getenv


class Database:
    def __init__(self):
        self.connection = None
        self.models = {}

        self.template = None

        self.name = None
        self.host = None
        self.port = None
        self.user = None

    @staticmethod
    def buildFromTemplate(template_path):
        with open(template_path, "r") as template_file:
            template = json.load(template_file)

        # we will build it step by step
        database = Database()
        database.template = template

        # first build the connection (required for all tables/models)
        for k,v in template["database"].items():
            if v == "ENV":
                template["database"][k] = getenv(k)

        database.name = template["database"]["name"]
        database.host = template["database"]["host"]
        database.port = template["database"]["port"]
        database.user = template["database"]["username"]

        database.connection = DatabaseConnection(
            db_name=database.name,
            host=database.host,
            port=database.port,
            username=database.user,
            password=template["database"]["password"]
        )

        template["database"]["password"] = "ENV" # we never store the password !!! it should always be provided by ENV

        database.connection.connect()

        for table in template["tables"]:
            pass

        database.toto = None
        database.toto.tata = "titi"
        print(database.toto.tata)
        return database



if __name__ == "__main__":
    db = Database.buildFromTemplate("./test_template.json")

