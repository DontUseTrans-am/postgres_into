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
if __name__=="__main__":
    c=Connection()
    for x,y in c.dictionary.items():
        print(x,y)

