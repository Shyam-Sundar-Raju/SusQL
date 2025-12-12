"""
SuSql - A simple SQL database management system

Features:
1. Create table
2. Drop table
3. Insert records
4. Delete records
5. Select records
6. Update records
7. Truncate table
8. Show tables
9. See table
10. Save database
11. Load database

Syntax:
1. create <table_name> (<column1>,<column2>,...,<columnN>) primarykey=<column_name>/none;
2. drop <table_name>;
3. insert <table_name> (<value1>,<value2>,...,<valueN>) (<value1>,<value2>,...,<valueN>)...;
4. delete <table_name> where <condition>;
5. select <table_name> */<column1>,<column2>,...,<columnN> where <condition>;
6. update <table_name> <column1>=<value1>,<column2>=<value2>,...,<columnN>=<valueN> where <condition>;
7. truncate <table_name>;
8. show tables;
9. see <table_name>;
10. save <file_name>;
11. load <file_name>;
12. new database;
13. exit;

Note:
1. Primary key is optional.
2. Conditions can be of the form <column_name><operator><value> or <column_name>_in_(<value1>,<value2>,...,<valueN>) or <column_name>_like_<value>.
3. Conditions can be combined using 'and' and 'or'.
4. The password is 'password'.

Data structures used:
1. Each record is stored as a node.
2. Each node has an array, containing the values of the columns. 
3. Each table is stored as a linked list of nodes, with the head node storing the column names and primary key.
4. The tables are stored in a dictionary with the table name as the key.

Time complexity:
1. Create table: O(1)
2. Drop table: O(1)
3. Insert records: O(N)     N is the number of records
4. Delete records: O(N)     N is the number of records
5. Select records: O(N)     N is the number of records
6. Update records: O(N)     N is the number of records
7. Truncate table: O(1)
8. Show tables: O(1)
9. See table: O(1)
10. Save database: O(N*M)    N is the number of tables and M is the number of records
11. Load database: O(N*M)    N is the number of tables and M is the number of records
12. New database: O(1)

Space complexity: O(N*M)    N is the number of tables and M is the number of records

Team contribution:
1. CB.SC.U4CSE23003 - Create, Insert, Update
2. CB.SC.U4CSE23020 - Where, Delete, Drop
3. CB.SC.U4CSE23022 - Select, Truncate
4. CB.SC.U4CSE23049 - Show, See, Save, Load, New
"""

import gc
from tabulate import tabulate
from getpass import getpass

class DBMS:
    def __init__(self):
        self.tables={}

    def create_table(self,str_list):    #Create table
        name=str_list[0]
        if name in self.tables:
            raise C_error("Error: Table already exists")
        columns=str_list[1:-1]
        columns[0]=columns[0][1:]
        p_key=str_list[-1][11:]
        for i in range(len(columns)):
            columns[i]=columns[i][:-1]
        if p_key=="none":
            self.tables[name]=Node(columns,None)
        else:
            if p_key not in columns:
                raise C_error("Error: Primary key not in columns")
            else:
                self.tables[name]=Node(columns,p_key)
        return print("Table created successfully")

    def truncate_table(self,str_list):    #Truncate table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        self.tables[name].next=None
        gc.collect()
        return print("Table truncated successfully")

    def drop_table(self,str_list):    #Drop table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        del self.tables[name]
        return print("Table dropped successfully")

    def insert(self,str_list):    #Insert records into table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        head=self.tables[name]
        length=len(head.attr)
        p_key_index=head.attr.index(head.p_key) if head.p_key else None
        for i in str_list[1:]:
            attr=i.split(",")
            attr[0]=attr[0][1:]
            attr[-1]=attr[-1][:-1]
            if len(attr)>length:
                raise C_error("Error: Too many values")
            if p_key_index!=None:
                node=head.next
                while node!=None:
                    if node.attr[p_key_index]==attr[p_key_index]:
                        raise C_error("Error: Duplicate primary key")
                    node=node.next
            
            record=Node(attr, None)
            record.next=head.next
            head.next=record
        
        return print(f"{len(str_list) - 1} records inserted successfully")
    
    def select(self,str_list):    #Select records from table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        head=self.tables[name]
        columns=str_list[1].split(",")
        sel_col=[0 for _ in range(len(head.attr))]
        if columns[0]=="*":
            sel_col=[1 for _ in range(len(head.attr))]
            columns=head.attr
        else:
            for i in columns:
                if i not in head.attr:
                    raise C_error("Error: Column does not exist")
                else:
                    sel_col[head.attr.index(i)]=1
        if "where" in str_list:
            condition=str_list[3:]
            nodes=self.where(condition,head)
        else:
            nodes=self.return_all(head)
        selected=[]
        for node in nodes:
            sel_val=[]
            for j in range(len(sel_col)):
                if sel_col[j]==1:
                    sel_val.append(node.attr[j])
            selected.append(sel_val)
        return print(tabulate(selected,columns,tablefmt="fancy_grid"))

    def update(self,str_list):    #Update records in table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        head=self.tables[name]
        command=str_list[1]
        columns=[]
        values=[]
        command=command.split(",")
        for i in range(0,len(command)):
            columns.append(command[i].split("=")[0])
            values.append(command[i].split("=")[1])
        if "where" in str_list:
            condition=str_list[3:]
            nodes=self.where(condition,head)
        else:
            nodes=self.return_all(head)
        length=len(nodes)
        for i in columns:
            if i not in head.attr:
                raise C_error("Error: Column does not exist")
        for node in nodes:
            for i in range(len(columns)):
                node.attr[head.attr.index(columns[i])]=values[i]
        return print(f"{length} records updated successfully")

    def delete(self,str_list):    #Delete records from table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        head=self.tables[name]
        if "where" in str_list:
            condition=str_list[2:]
            nodes=self.where(condition,head)
        else:
            nodes=self.return_all(head)
        length=len(nodes)
        while head.next:
            if head.next in nodes:
                head.next=head.next.next
            else:
                head=head.next
        gc.collect()
        return print(f"{length} records deleted successfully")
    
    def where(self,str_list,head):    #Where clause for select, update and delete
        conditions=str_list[::2]
        combine=str_list[1::2]
        operators=['>','<','==','>=','<=','_in_','_like_']
        conditionResults=[]
        for condition in conditions:
            iso=False
            op=None
            node=head.next
            for operator in operators:
                if operator in condition:
                    iso=True
                    op=operator
                    break
            resultSet=[]
            if iso!=True:
                raise C_error("Error: No operator found")
            else:
                column=condition.split(op)
                c1=column[0]
                value=column[1]
                if op not in ['_in_','_like_']:
                    while node!=None:
                        if op=='>':
                            if node.attr[head.attr.index(c1)]>value:
                                resultSet.append(node)
                        elif op=='<':
                            if node.attr[head.attr.index(c1)]<value:
                                resultSet.append(node)
                        elif op=='==':
                            if node.attr[head.attr.index(c1)]==value:
                                resultSet.append(node)
                        elif op=='>=':
                            if node.attr[head.attr.index(c1)]>=value:
                                resultSet.append(node)
                        elif op=='<=':
                            if node.attr[head.attr.index(c1)]<=value:
                                resultSet.append(node)
                        node=node.next
                elif op=='_in_':
                    value=value[1:-1].split(",")
                    while node!=None:
                        if node.attr[head.attr.index(c1)] in value:
                            resultSet.append(node)
                        node=node.next
                elif op=='_like_':
                    while node!=None:
                        if value in node.attr[head.attr.index(c1)]:
                            resultSet.append(node)
                        node=node.next
            conditionResults.append(resultSet)
        while combine:
            set1=conditionResults.pop(0)
            set2=conditionResults.pop(0)
            if combine[0]=='or':
                result=set1+set2
                unique_list=[]
                [unique_list.append(item) for item in result if item not in unique_list]
                conditionResults.insert(0, unique_list)
            elif combine[0]=='and':
                unique_list=[]
                [unique_list.append(item) for item in set1 if item in set2]
                conditionResults.insert(0, unique_list)
            combine.pop(0)

        finalResult=conditionResults[0] if conditionResults else []
        return finalResult

    def show_tables(self):    #Show tables
        for i in self.tables:
            print(i)
        
    def see_table(self,str_list):    #Describe table
        name=str_list[0]
        if name not in self.tables:
            raise C_error("Error: Table does not exist")
        head=self.tables[name]
        return print(head.attr,"Primary key= ",head.p_key)
        
    def save_state(self,str_list):    #Save database as txt file
        name=str_list[0]
        f=open("C:/Users/shyam/Desktop/College/dsa/"+name+".txt","w+")
        for i in self.tables:
            f.write(i+"\n")
            head=self.tables[i]
            f.write(str(head.attr)+" "+str(head.p_key).lower()+"\n")
            node=head.next
            while node:
                f.write(str(node.attr)+"\n")
                node=node.next
            f.write("\n")
        f.close()
        return print("State saved successfully")
    
    def load_state(self,str_list):    #Load database from txt file
        name=str_list[0]
        f=open("C:/Users/shyam/Desktop/College/dsa/"+name+".txt","r+")
        self.tables={}
        lis=f.readlines()
        new_table=1
        name=""
        head=None
        for i in lis:
            if new_table==1:
                name=i[:-1]
                new_table=0
                continue
            elif i=="\n":
                new_table=1
                continue
            elif new_table==0:
                i=i[:-1]
                columns=i.split(" ")[:-1]
                columns[0]=columns[0][1:]
                for val in range(len(columns)):
                    columns[val]=columns[val][1:-2]
                p_key=i.split(" ")[-1]
                if p_key=="none":
                    self.tables[name]=Node(columns,None)
                else:
                    self.tables[name]=Node(columns,p_key)
                new_table=-1
                head=self.tables[name]
                continue
            attr=eval(i)
            record=Node(attr,None)
            record.next=head.next
            head.next=record
        f.close()
        return print("State loaded successfully")


    def new_database(self):    #Create new database
        self.tables={}
        return print("New database created successfully")

    def return_all(self,head):    #Return all records from table
        nodes=[]
        node=head.next
        while node!=None:
            nodes.append(node)
            node=node.next
        return nodes

class Node:    #Each record is stored as a node
    def __init__(self,attr,p_key):
        self.attr=attr
        self.next=None
        self.p_key=p_key

class C_error(Exception):    #Custom error class
    pass

db=DBMS()
password=getpass("Enter password: ")
if password!="password":
    print("Incorrect password")
    exit()
while True:
    try:
        command=input("\n>>>")
        while command[-1]!=";":
            command+=" "+input("...")
        command=command[:-1]
        command.lower()
        command=command.split(" ")
        if command[0]=="create":
            db.create_table(command[1:])
        elif command[0]=="drop":
            db.drop_table(command[1:])
        elif command[0]=="insert":
            db.insert(command[1:])
        elif command[0]=="delete":
            db.delete(command[1:])
        elif command[0]=="select":
            db.select(command[1:])
        elif command[0]=="update":
            db.update(command[1:])
        elif command[0]=="truncate":
            db.truncate_table(command[1:])
        elif command[0]=="show":
            db.show_tables()
        elif command[0]=="see":
            db.see_table(command[1:])
        elif command[0]=="save":
            db.save_state(command[1:])
        elif command[0]=="load":
            db.load_state(command[1:])
        elif command[0]=="new":
            db.new_database()
        elif command[0]=="exit":
            break
        else:
            raise C_error("Error: Invalid command")
    except Exception as e:
        print("There is some error in the syntax. Please check the command again.")
        print(e)