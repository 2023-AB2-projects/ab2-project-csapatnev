# Official Server side for DBMSName
# ToDo:
#       Create Classes for individual syntax keywords
#       Create a parsing function
#       Establish connection to client side, check for errors / error messages
#       CREATE
#       DATABASE
#       TABLE
#       DROP
#       CREATE INDEX
#       xml file handling / reading / writing
#       Required data types: int, float, bit, date, datetime, 
#                            varchar(or string without specified length)

import socket as sck
import commands as cmd
import myparser.parsing_syntax as prs
import mongoHandler as mh
import pdb

import xml.etree.ElementTree as ET

from protocol.simple_protocol import *

HOST = '127.0.0.1'
PORT = 6969 # nice

DATABASE_IN_USE = "MASTER"

RESPONSE_MESSAGES = []
TABLES_TO_SELECT = []


# CREATE DATABASE database_name;
def create_database_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = res['database_name']
    
    ret_val, err_msg = cmd.create_database(db_name)
    if ret_val >= 0:
        global DATABASE_IN_USE
        DATABASE_IN_USE = db_name
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# CREATE TABLE table_name (col_definitions); 
def create_table_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    columns = res['column_definitions']
    pk = res['primary_keys']
    fk = res['references']
    uk = res['unique']

    ret_val, err_msg = cmd.create_table(db_name, table_name, columns, pk, fk, uk)
    if ret_val >= 0:
        response_msg = err_msg 

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        print(f'{ret_val} : {err_msg}')
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# CREATE INDEX <index_name> on table_name (col1, col2, ...);
def create_index_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    index_name = res['index_name'].upper()
    table_name = res['table_name'].upper()
    columns = res['columns']
    
    ret_val, err_msg = cmd.create_index(mongoclient, db_name, table_name, index_name, columns)
    if ret_val >= 0:
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DROP DATABASE database_name
def drop_database_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = res['database_name']
    
    ret_val, err_msg = cmd.drop_database(db_name, mongoclient)
    if ret_val >= 0:
        global DATABASE_IN_USE
        DATABASE_IN_USE = "MASTER"
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DROP TABLE table_name;
def drop_table_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    
    ret_val, err_msg = cmd.drop_table(db_name, table_name, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')
        
    return ret_val


# USE DATABASE db_name;
def use_request(mode, res, mongoclient, connection_socket: sck.socket):
    global DATABASE_IN_USE
    DATABASE_IN_USE = res['database_name']
    return 0


# INSERT INTO table_name (col1, col2, col3, ...) values (val1, val2, val3, ...);
def insert_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    columns = res['columns']
    values = res['values']
    
    ret_val, err_msg = cmd.insert_into(db_name, table_name, columns, values, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg
        
        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else: 
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# DELETE FROM table_name WHERE condition;
def delete_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    table_name = res['table_name']
    filter_conditions = res['condition']
    
    ret_val, err_msg = cmd.delete_from(db_name, table_name, filter_conditions, mongoclient)
    if ret_val >= 0:
        response_msg = err_msg

        if mode == 'debug': print(response_msg)
        else: RESPONSE_MESSAGES.append(response_msg)
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# UPDATE table_name SET (col1 = value1, col2 = value2, ...) WHERE condition;
def update_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE

    table_name = res['table_name']
    return 0


# SELECT columns FROM table_name <JOIN join_condition> <WHERE condition> <GROUP BY col1, col2, ...>;
def select_request(mode, res, mongoclient, connection_socket: sck.socket):
    db_name = DATABASE_IN_USE
    
    select_clause = res['select_clause']
    select_distinct = res['select_distinct']
    from_clause = res['from_clause']
    join_clause = res['join_clause']
    where_clause = res['where_clause']
    groupby_clause = res['groupby_clause']
    
    ret_val, err_msg = cmd.select(db_name, select_clause, select_distinct, from_clause, join_clause, where_clause, groupby_clause, mongoclient)
    if ret_val >= 0:
        response_msg = 'Selected successfully!'
        
        if mode == 'debug': print(err_msg)
        else:
            RESPONSE_MESSAGES.append(response_msg)
            RESPONSE_MESSAGES.append('select') 
            RESPONSE_MESSAGES.append(str(err_msg)) 
    else:
        if mode == 'debug': print(err_msg)
        else:
            send_one_message(connection_socket, err_msg)
            send_one_message(connection_socket, 'breakout')

    return ret_val


# parse through the whole input file, and search for errors            
def first_parse(syntax: str, mode = ''):
    error_found = False
    error_messages = []

    for res in syntax:
        if res['code'] < 0:
            error_found = True
            error_messages.append(res['message'])
            
            if mode == 'debug': print(res)
    
    return error_found, error_messages


REQUEST_HANDLING = {
    1: create_database_request,
    2: create_table_request,
    3: create_index_request,
    4: drop_database_request,
    5: drop_table_request,
    6: use_request,
    7: insert_request,
    8: delete_request,
    9: update_request,
    10: select_request
}


def test_syntax(syntax: str, connection_socket: sck.socket, mode=''):
    # first parse:
    error_found, error_messages = first_parse(syntax, mode)
    if error_found:
        if mode == 'debug': print("ERROR FOUND AT FIRST PARSE!")
        
        for msg in error_messages:
            if mode == 'debug': print(msg)
            else: send_one_message(connection_socket, msg)
        
        print("breaking out")
        
        if mode != 'debug':
            send_one_message(connection_socket, 'breakout')
        return -1

    global DATABASE_IN_USE
    global TABLES_TO_SELECT
    mongodb, mongoclient = mh.connect_mongo(DATABASE_IN_USE)

    status_code = 0
    for res in syntax: 
        status_code = REQUEST_HANDLING[res['code']](mode, res, mongoclient, connection_socket) 
        if status_code < 0: break      

    if status_code == 0:
        for response_msg in RESPONSE_MESSAGES:
            if mode != 'debug':
                send_one_message(connection_socket, response_msg)

        if mode != 'debug':
            send_one_message(connection_socket, 'breakout')
        

def create_tree_from_xml():
    response = {}
    tree = ET.parse('databases.xml')
    root = tree.getroot()

    for database in root.findall('Database'):
        response[database.attrib['name']] = []
        table_response = {}
        for table in database.findall('.//Table'):
            table_response[table.attrib['name']] = []
            for attribute in table.findall('.//Attribute'):
                table_response[table.attrib['name']].append(attribute.attrib['attributeName'])
            response[database.attrib['name']].append(table_response)
            table_response = {}

    return response


# CLIENT's 'run' COMMAND:
def handle_run_request(connection_socket):
    global RESPONSE_MESSAGES
    RESPONSE_MESSAGES = []
            
    message = recv_one_message(connection_socket).decode()

    full_request = prs.handle_my_sql_input(message)
    test_syntax(full_request, connection_socket)


# CLIENT's 'console' COMMAND:
def handle_console_request(connection_socket):
    global RESPONSE_MESSAGES
    RESPONSE_MESSAGES = []
            
    message = recv_one_message(connection_socket).decode()

    full_request = prs.handle_my_sql_input(message)
    test_syntax(full_request, connection_socket)


# CLIENT's 'tree' COMMAND:
def handle_tree_request(connection_socket):
    response = create_tree_from_xml()

    send_one_message(connection_socket, str(response))
    send_one_message(connection_socket, 'breakout')


def server_side():
    global DATABASE_IN_USE
    global RESPONSE_MESSAGES

    HOST = '127.0.0.1'
    PORT = 6969

    server_socket = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
    server_socket.bind((HOST, PORT))

    server_socket.listen(1)
    print('The server is available')

    connection_socket, addr = server_socket.accept()
    print('The client has connected to the server', addr)

    while True:
        request = recv_one_message(connection_socket).decode()

        if request == 'kill yourself':
            break

        if request == 'run':
            handle_run_request(connection_socket)
            continue

        if request == 'console':
            handle_console_request(connection_socket)
            continue
        
        if request == 'tree':
            handle_tree_request(connection_socket)
            continue


if __name__ == "__main__":
    server_side()

    # syntax = """
    # USE UNIVERSITY;

    # DROP DATABASE UNIVERSITY;

    # create database University;

    # USE University;

    # CREATE TABLE credits (
    #     CreditNr int PRIMARY KEY,
    #     CName varchar(30) UNIQUE
    # );

    # CREATE TABLE disciplines (
    #     DiscID varchar(5) PRIMARY KEY,
    #     DName varchar(30) UNIQUE,
    #     CreditNr int REFERENCES credits(CreditNr)
    # );

    # INSERT INTO Credits (CreditNr, CName) VALUES (1, 'Mathematics');
    # INSERT INTO Credits (CreditNr, CName) VALUES (2, 'Physics');
    # INSERT INTO Credits (CreditNr, CName) VALUES (3, 'Chemistry');
    # INSERT INTO Credits (CreditNr, CName) VALUES (4, 'Biology');
    # INSERT INTO Credits (CreditNr, CName) VALUES (5, 'Geography');
    # INSERT INTO Credits (CreditNr, CName) VALUES (6, 'History');
    # INSERT INTO Credits (CreditNr, CName) VALUES (7, 'English');
    # INSERT INTO Credits (CreditNr, CName) VALUES (8, 'German');

    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('MATH', 'Mathematics', 1);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('CHEM', 'Chemistry', 3);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('PHY', 'Physics', 2);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('BIO', 'Biology', 4);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('GEO', 'AISYD', 6);
    
    # Create index asd on disciplines (DName, CreditNr);

    # /* constraint violation: */
    # /* INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('MATH2', 'Mathematics', 1); */

    # /* correct delete from: */
    # /* DELETE FROM Disciplines WHERE DiscID = 'MATH'; */

    # /* constraint violation delete from: */
    # /* -- Create a table for subjects */
    
    # CREATE TABLE subjects (
    #     subject_id int PRIMARY KEY,
    #     subject_name varchar(30)
    # );

    # /* Create a table for students with a foreign key referencing subjects */
    
    # CREATE TABLE students (
    #     student_id int PRIMARY KEY,
    #     student_name varchar(30),
    #     subject_id int REFERENCES subjects(subject_id)
    # );

    # /* -- Insert a subject */
    # INSERT INTO subjects (subject_id, subject_name) VALUES (1, 'Mathematics');

    # INSERT INTO students (student_id, student_name, subject_id) VALUES (1, 'John Doe', 1);

    # /* -- Attempt to delete the subject which is being referenced by the student */
    # /* DELETE FROM subjects WHERE subject_id = 1; */

    # SELECT DName AS DiscName, CreditNr FROM Disciplines WHERE CreditNr > 1 AND CreditNr < 4;
    # """

    
    # syntax2 = """
    # USE UNIVERSITY;
    # DROP DATABASE UNIVERSITY;
    # create database University;
    # USE University;

    # CREATE TABLE credits (
    #     CreditNr int,
    #     CName varchar(30) UNIQUE,
    #     PRIMARY KEY (CreditNr, CName)
    # );

    # INSERT INTO Credits (CreditNr, CName) VALUES (1, 'Matematics');
    # INSERT INTO Credits (CreditNr, CName) VALUES (2, 'Physics');
    # INSERT INTO Credits (CreditNr, CName) VALUES (3, 'Chemistry');
    # INSERT INTO Credits (CreditNr, CName) VALUES (4, 'Biology');

    # SELECT CName AS CC, CreditNr FROM Credits WHERE CreditNr > 1;
    # """

    # syntax3 = """
    # DROP DATABASE SCHOOL;
    # CREATE DATABASE SCHOOL;

    # CREATE TABLE subjects (
    #     subject_id int PRIMARY KEY,
    #     subject_name varchar(30)
    #     );

    # /* Create a table for students with a foreign key referencing subjects */
    
    # CREATE TABLE students (
    #     student_id int PRIMARY KEY,
    #     student_name varchar(30),
    #     subject_id int REFERENCES subjects(subject_id)
    # );

    # /* -- Insert a subject */
    # INSERT INTO subjects (subject_id, subject_name) VALUES (1, 'Mathematics');

    # INSERT INTO students (student_id, student_name, subject_id) VALUES (1, 'John Doe', 1);

    # /* -- Attempt to delete the subject which is being referenced by the student */
    # DELETE FROM subjects WHERE subject_id = 1;
    # """

    # syntax4 = """
    # DROP DATABASE UNIVERSITY;
    # CREATE DATABASE UNIVERSITY;

    # CREATE TABLE disciplines (
    #     DiscID varchar(5) PRIMARY KEY,
    #     DName varchar(30),
    #     CreditNr int
    # );

    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('MATH', 'Matematics', 1);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('CHEM', 'Matematics', 3);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('PHY', 'Matematics', 2);
    # INSERT INTO Disciplines (DiscID, DName, CreditNr) VALUES ('BIO', 'Biology', 4);

    # SELECT * FROM Disciplines WHERE CreditNr > 4;
    # """

    # syntax5 = """
    # DROP DATABASE ONLINE_SHOP;
    # CREATE DATABASE ONLINE_SHOP;
    # USE ONLINE_SHOP;

    # CREATE TABLE Orders (
    #     Order_ID INT PRIMARY KEY,
    #     Product_ID INT,
    #     Quantity INT
    # );

    # CREATE TABLE Products (
    #     Product_ID INT PRIMARY KEY,
    #     Product_Name VarChar(255),
    #     Category_ID INT
    # );

    # CREATE TABLE Categories (
    #     Category_ID INT PRIMARY KEY,
    #     Category_Name VarChar(255)
    # );

    # CREATE INDEX orders_product_id ON Orders(Product_ID);
    # CREATE INDEX products_category_id ON Products(Category_ID);
    
    # INSERT INTO Orders (Order_ID, Product_ID, Quantity) VALUES (1, 101, 5);
    # INSERT INTO Orders (Order_ID, Product_ID, Quantity) VALUES (2, 102, 10);
    # INSERT INTO Orders (Order_ID, Product_ID, Quantity) VALUES (3, 103, 2);

    # INSERT INTO Products (Product_ID, Product_Name, Category_ID) VALUES (101, 'iPhone', 201);
    # INSERT INTO Products (Product_ID, Product_Name, Category_ID) VALUES (102, 'MacBook', 202);
    # INSERT INTO Products (Product_ID, Product_Name, Category_ID) VALUES (103, 'iPad', 201);


    # INSERT INTO Categories (Category_ID, Category_Name) VALUES (201, 'Electronics');
    # INSERT INTO Categories (Category_ID, Category_Name) VALUES(202, 'Computers');

    # SELECT *
    # FROM Orders
    # JOIN Products ON Orders.Product_ID = Products.Product_ID
    # JOIN Categories ON Products.Category_ID = Categories.Category_ID;

    # """

    # syntax6 = """
    # drop database University;
    # create database University;
    # USE University;

    # CREATE TABLE disciplines (
    # DiscID varchar(5) PRIMARY KEY,
    # DName varchar(30),
    # CreditNr int
    # );


    # CREATE INDEX MixedIndex on disciplines (DiscID, CreditNr);


    # /*Data for the table disciplines */

    # insert into disciplines (DiscID,DName,CreditNr) values ('DB1','Databases 1', 7);
    # insert into disciplines (DiscID,DName,CreditNr) values ('DS','Data Structures',6);
    # insert into disciplines (DiscID,DName,CreditNr) values ('CP','C Programming',8);
    # insert into disciplines (DiscID,DName,CreditNr) values ('ST','Statistics',5);
    # insert into disciplines (DiscID,DName,CreditNr) values ('LT','Lattice Theory',8);
    # insert into disciplines (DiscID,DName,CreditNr) values ('OOP','Object Oriented Programming',6);
    # insert into disciplines (DiscID,DName,CreditNr) values ('AI','Artificial Intelligence',8);
    # insert into disciplines (DiscID,DName,CreditNr) values ('OS','Operating Systems',3);
    # insert into disciplines (DiscID,DName,CreditNr) values ('DB2','Databases 2',6);
    # insert into disciplines (DiscID,DName,CreditNr) values ('MA','Math Analysis',7);
    # insert into disciplines (DiscID,DName,CreditNr) values ('SI','Software Engineering',6);
    # insert into disciplines (DiscID,DName,CreditNr) values ('AL','Algebra',4);


    # /*Table structure for table specialization */


    # CREATE TABLE specialization (
    # SpecID varchar(5) PRIMARY KEY,
    # SpecName varchar(40),
    # Language varchar(20) 
    # );

    # /*Data for the table specialization */

    # insert into specialization (SpecID,SpecName,Language) values ('I','Informatics','English');
    # insert into specialization (SpecID,SpecName,Language) values ('MI','Mathematics Informatics','English');
    # insert into specialization (SpecID,SpecName,Language) values ('M','Mathematics','English');
    # insert into specialization (SpecID,SpecName,Language) values ('P','Physics','German');
    # insert into specialization (SpecID,SpecName,Language) values ('CM','Computational Mathematics','German');
    # insert into specialization (SpecID,SpecName,Language) values ('A','Automatization','French');
    # insert into specialization (SpecID,SpecName,Language) values ('ING','Engineering','French');
    # insert into specialization (SpecID,SpecName,Language) values ('C','Calculators','French');
    # insert into specialization (SpecID,SpecName,Language) values ('MP','Mathematics and Physics','German');

    # """

    # syntax7 = """
    #     drop database tmp;
    #     create database tmp;
    #     use tmp;

    #     create table Subjects (
    #         SubjectID int primary key,
    #         SubjectName varchar(255),
    #         Courses int
    #     );

    #     create table Felhasznalok ( 
    #         FID int primary key,
    #         FName varchar(255),
    #         FType varchar(255)
    #     );


    #     create table Resources (
    #         RID int primary key,
    #         RName varchar(255),
    #         SubjectID int REFERENCES Subjects (SubjectID)
    #     );


    #     insert into Subjects (SubjectID, SubjectName, Courses) values (1, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (2, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (3, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (4, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (5, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (6, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (7, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (8, 'asd', 10);
    #     insert into Subjects (SubjectID, SubjectName, Courses) values (9, 'asd', 10);


    #     insert into Felhasznalok (FID, FName, FType) values (1, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (2, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (3, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (4, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (5, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (6, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (7, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (8, 'Name', 'client');
    #     insert into Felhasznalok (FID, FName, FType) values (9, 'Name', 'client');


    #     insert into Resources (RID, RName, SubjectID) values (1, 'video', 1);
    #     insert into Resources (RID, RName, SubjectID) values (2, 'video', 1);
    #     insert into Resources (RID, RName, SubjectID) values (3, 'video', 1);
    #     insert into Resources (RID, RName, SubjectID) values (4, 'video', 4);
    #     insert into Resources (RID, RName, SubjectID) values (5, 'video', 5);
    #     insert into Resources (RID, RName, SubjectID) values (6, 'video', 6);
    #     insert into Resources (RID, RName, SubjectID) values (7, 'video', 7);
    #     insert into Resources (RID, RName, SubjectID) values (8, 'video', 8);
    #     insert into Resources (RID, RName, SubjectID) values (9, 'video', 9);
    # """

    # syntax8 = """
    # DROP DATABASE UNIVERSITY;
    # CREATE DATABASE UNIVERSITY;
    # USE UNIVERSITY;

    # CREATE TABLE Students (
    #     StudentID INT PRIMARY KEY,
    #     FirstName VARCHAR(100),
    #     LastName VARCHAR(100),
    #     Age INT
    # );

    # INSERT INTO Students (StudentID, FirstName, LastName, Age) VALUES (1, 'John', 'Doe', 20);
    # INSERT INTO Students (StudentID, FirstName, LastName, Age) VALUES (2, 'Jane', 'Smith', 22);
    # INSERT INTO Students (StudentID, FirstName, LastName, Age) VALUES (3, 'Bob', 'Johnson', 21);
    # INSERT INTO Students (StudentID, FirstName, LastName, Age) VALUES (4, 'Alice', 'Davis', 23);
    # INSERT INTO Students (StudentID, FirstName, LastName, Age) VALUES (5, 'Charlie', 'Brown', 20);

    # CREATE TABLE Courses (
    #     CourseID INT PRIMARY KEY,
    #     CourseName VARCHAR(100),
    #     CreditHours INT
    # );

    # INSERT INTO Courses (CourseID, CourseName, CreditHours) VALUES (1, 'Math', 3);
    # INSERT INTO Courses (CourseID, CourseName, CreditHours) VALUES (2, 'English', 4);
    # INSERT INTO Courses (CourseID, CourseName, CreditHours) VALUES (3, 'Physics', 4);
    # INSERT INTO Courses (CourseID, CourseName, CreditHours) VALUES (4, 'Chemistry', 4);
    # INSERT INTO Courses (CourseID, CourseName, CreditHours) VALUES (5, 'History', 3);

    # CREATE TABLE Enrollments (
    #     StudentID INT,
    #     CourseID INT,
    #     Grade INT,
    #     PRIMARY KEY (StudentID, CourseID)
    # );

    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (1, 1, 85);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (1, 2, 90);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (2, 1, 95);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (2, 3, 80);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (3, 2, 88);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (3, 3, 85);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (4, 2, 80);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (4, 3, 92);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (5, 1, 90);
    # INSERT INTO Enrollments (StudentID, CourseID, Grade) VALUES (5, 2, 85);

    # SELECT avg(Age) AS AverageAge FROM Students;

    # SELECT count(StudentID) AS StudentCount, avg(Age) AS AverageAge FROM Students;

    # SELECT CourseID, avg(Grade) AS AverageGrade 
    # FROM Enrollments 
    # GROUP BY CourseID;
        
    # SELECT CourseID, count(StudentID) AS StudentCount, min(Grade)
    # FROM Enrollments 
    # GROUP BY CourseID;

    # """

    # syntax8_5 = """
    #     DROP DATABASE UNIVERSITY2;
    #     CREATE DATABASE UNIVERSITY2;

    #     CREATE TABLE disciplines (
    #         DiscID varchar(5) PRIMARY KEY,
    #         DName varchar(30),
    #         CreditNr int
    #     );

    #     CREATE TABLE students (
    #         StudID int PRIMARY KEY,
    #         GroupId int,
    #         StudName varchar(20),
    #         Email varchar(20) 
    #     );

    #     CREATE TABLE marks (
    #     StudID int(10) REFERENCES students (StudID),
    #     DiscID varchar(20) REFERENCES disciplines (DiscID),
    #     Mark int,
    #     PRIMARY KEY (StudID,DiscID)
    #     );

    #     INSERT INTO disciplines (DiscID, DName, CreditNr) VALUES ('DISC1', 'Computer Science', 4);
    #     INSERT INTO disciplines (DiscID, DName, CreditNr) VALUES ('DISC2', 'Mathematics', 3);
    #     INSERT INTO disciplines (DiscID, DName, CreditNr) VALUES ('DISC3', 'Physics', 4);
    #     INSERT INTO disciplines (DiscID, DName, CreditNr) VALUES ('DISC4', 'Chemistry', 3);

        
    #     INSERT INTO students (StudID, GroupId, StudName, Email) VALUES (1, 101, 'John Doe', 'johndoe@example.com');
    #     INSERT INTO students (StudID, GroupId, StudName, Email) VALUES (2, 101, 'Jane Doe', 'janedoe@example.com');
    #     INSERT INTO students (StudID, GroupId, StudName, Email) VALUES (3, 102, 'Alice Smith', 'alicesmith@example.com');
    #     INSERT INTO students (StudID, GroupId, StudName, Email) VALUES (4, 102, 'Bob Johnson', 'bobjohnson@example.com');

    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (1, 'DISC1', 85);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (1, 'DISC2', 90);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (2, 'DISC1', 95);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (2, 'DISC3', 88);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (3, 'DISC2', 80);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (3, 'DISC4', 78);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (4, 'DISC1', 70);
    #     INSERT INTO marks (StudID, DiscID, Mark) VALUES (4, 'DISC3', 75);

    # """

    # syntax9 = """
    # USE UNIVERSITY;

    # delete from marks where StudID = 50 and DiscID = 'OOP';
    # delete from marks where StudID = 49 and DiscID = 'OOP';

    # delete from students where StudID = 50;
    # delete from students where GroupId = 531;
    
    # """

    # syntax = prs.handle_my_sql_input(syntax9)
    # test_syntax(syntax, '', 'debug')