import yaml

class Config:
    def __init__(self, configyamlfile):
        self.settings = dict()
        with open(configyamlfile, "r") as stream:
            try:
                self.settings = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
            
    def validate(self):
        if self.User and self.Password and self.Account and \
            self.Database and self.Warehouse and self.Schema and \
            self.ACCESS_KEY and self.SECRET_ACCESS_KEY and self.REGION:
            print('Config file is valid')
            return True

        return False


    @property
    def User(self):
        return self.settings["user"]

    @property
    def Password(self):
        return self.settings["password"]

    @property
    def Account(self):
        return self.settings["account"]

    @property
    def Database(self):
        return self.settings["database"]

    @property
    def Warehouse(self):
        return self.settings["warehouse"]

    @property
    def Schema(self):
        return self.settings["schema"]
    
    @property
    def ACCESS_KEY(self):
        return self.settings["ACCESS_KEY"]
    
    @property
    def SECRET_ACCESS_KEY(self):
        return self.settings["SECRET_ACCESS_KEY"]

    @property
    def REGION(self):
        return self.settings["REGION"]
    