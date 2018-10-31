import csv

class Connection(object):
    def __init__(self,filename="setting.csv"):
        self.dictionary={}
        with open(filename,"r") as file:
            reader=csv.reader(file)
            for row in reader:
                value=row.pop()
                key=row.pop()
                self.dictionary[key]=value
    def create(self):
        conn_sql=""
        for keys,values in self.dictionary.items():
            conn_sql+=keys+"="+values+" "
        return conn_sql 
if __name__=="__main__":
    c=Connection()
    print(c.create())

