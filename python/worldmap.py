import pyodbc 

class DB:
    def __init__(self):
         self.server = 'tcp:test-server.database.windows.net'
        self.database = '' 
        self.username = '' 
        self.password = '' 
        self.cnxn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server={your server name},1433;Database={your database};Uid={username}@test-server;Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        self.cursor = self.cnxn.cursor()

    def getConnection(self):
        return self.cursor

    def execute(self,query):
        self.cursor.execute(query)
        return self.cursor

    def insert(self,query):
        self.cursor.execute(query)
        self.cnxn.commit()
        return self.cursor
#Create the DB connection 
db = DB()

def createMap():
    global db
    
    #selecting tweets with coordinates
    output = db.execute("select tweetID, longi, lang from dbo.tweetstb where lang != 'None'")
    listID = []
    coordinates=[]

    for i in output :
        listID.append(i[0])
        coordinates.append([i[1],i[2]])
        
    emotion = []

    for j in listID:
        #selecting the emotion of tweet with coordinates
        output = db.execute("select emotion from dbo.emotiontb where tweetID= '{0}'".format(j))
        for k in output:
            emotion.append(k[0])

    listdic = []
    for (i, e) in zip(coordinates, emotion):
        
        #dictionary storing the coordinates & emotion of the tweet
        dic = {"z": 1, "lat": float(i[1]),"lon": float(i[0]), "code" : str(e)}
        listdic.append(dic)
        
    return (listdic)    



