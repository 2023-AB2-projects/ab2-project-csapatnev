# return (code) values:
#   1 - CREATE DATABASE
#   2 - CREATE TABLE
#   3 - CREATE INDEX
#   4 - DROP DATABASE
#   5 - DROP TABLE
#   6 - USE
#   7 - INSERT
#   8 - DELETE

import re


def parse_handle_invalid_object_type():
    return {
        'code': -1,
        'message': 'Invalid sql Object type!'
    }


def parse_handle_invalid_sql_command():
    return {
        'code': -1,
        'message': 'Invalid SQL command!'
    }


def parse_handle_invalid_syntax_for_creating_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating database!'
    }


def parse_handle_invalid_syntax_for_creating_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating a table.'
    }


def parse_handle_invalid_syntax_for_creating_index():
    return {
        'code': -1,
        'message': 'Invalid syntax for creating index!'
    }


def parse_handle_invalid_syntax_for_dropping_database():
    return {
        'code': -1,
        'message': 'Invalid syntax for dropping database!'
    }


def parse_handle_invalid_syntax_for_dropping_table():
    return {
        'code': -1,
        'message': 'Invalid syntax for dropping table!'
    }


def parse_handle_invalid_syntax_for_use():
    return {
        'code': -1,
        'message': 'Invalid syntax for use!'
    }


def parse_handle_no_input():
    return {
        'code': -1,
        'message': 'No sql command has been provided!'
    }


def parse_handle_invalid_syntax_for_inserting():
    return {
        'code': -1,
        'message': 'Invalid syntax for inserting!'
    }


def parse_handle_invalid_syntax_for_deleting():
    return {
        'code': -1,
        'message': 'Invalid syntax for deleting!'
    }


def parse_handle_condition(condition_str):
    condition_dict = {}
    logical_op = None

    operator_mapping = {
        '>': '$gt',
        '<': '$lt',
        '>=': '$gte',
        '<=': '$lte',
        '=': '$eq',
        '!=': '$ne',
    }

    if condition_str:
        clauses = re.findall(
            r"(\w+)\s*(=|!=|<=|>=|<|>)\s*('[^']*'|[^\s]+)", condition_str)
        clauses = [(clause[0], operator_mapping[clause[1]],
                    clause[2].strip("'")) for clause in clauses]
        logical_ops = re.findall(
            r"\b(AND|OR)\b", condition_str, flags=re.IGNORECASE)

        if logical_ops:
            logical_op = logical_ops[0].upper()

        if logical_op == 'AND':
            for column, operator, value in clauses:
                condition_dict[column] = {operator: value}
        elif logical_op == 'OR':
            condition_dict['$or'] = [{column: {operator: value}}
                                     for column, operator, value in clauses]
        elif len(clauses) == 1:
            column, operator, value = clauses[0]
            condition_dict[column] = {operator: value}
        else:
            raise ValueError("Invalid input or unsupported condition format.")

    print(condition_dict)


def parse_handle_create_database(syntax_in_sql):
    create_database_pattern = r'^create\s+database\s+(\w+)\s*;?$'
    match = re.match(create_database_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_database()
    else:
        database_name = match.group(1)
        return {
            'code': 1,
            'type': 'create',
            'object_type': 'database',
            'database_name': database_name
        }


def parse_handle_create_table(syntax_in_sql):
    create_table_pattern = r'^create\s+table\s+(\w+)\s*\(\s*(.*)\s*\)\s*;?$'
    match = re.match(create_table_pattern, syntax_in_sql,
                     flags=re.IGNORECASE | re.DOTALL)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_table()
    else:
        table_name = match.group(1)
        columns_definitions_str = match.group(2)

        column_definitions = []
        primary_keys = []
        composite_primary_keys = []
        references = []

        composite_pk_pattern = r'^(.*)\s+primary\s+key\s*\(\s*(.*)\s*\)\s*$'
        composite_pk_match = re.match(
            composite_pk_pattern, columns_definitions_str, flags=re.IGNORECASE | re.DOTALL)

        if composite_pk_match != None:
            primary_keys_str = composite_pk_match.group(2)
            for primary_key in primary_keys_str.split(','):
                composite_primary_keys.append(primary_key.strip())
            columns_definitions_str = composite_pk_match.group(
                1).rstrip().rstrip(',')

        for column_definition_str in columns_definitions_str.split(','):
            column_definition_pattern = r'^\s*(\w+)\s+([\w\(\)0-9]+)\s*(?:(.*)\s*)?$'
            column_definition_match = re.match(
                column_definition_pattern, column_definition_str.strip(), re.IGNORECASE)
            
            if column_definition_match != None:
                data_type_pattern = r'^\s*(int|varchar)(?:\(.*\))\s*$'
                data_type_match = re.match(
                    data_type_pattern, column_definition_match.group(2), re.IGNORECASE)

                if data_type_match is None:
                    data_type_pattern = r'^\s*(int|varchar|bit|date|datetime|float)\s*$'
                    data_type_match = re.match(
                        data_type_pattern, column_definition_match.group(2), re.IGNORECASE)
                    if data_type_match is None:
                        return parse_handle_invalid_syntax_for_creating_table()

                column_name = column_definition_match.group(1)
                data_type = data_type_match.group(1)
                column_definitions.append((column_name, data_type))

                pk_pattern = r'^\s*primary\s+key\s*$'
                match_pk = re.match(
                    pk_pattern, column_definition_match.group(3), re.IGNORECASE)

                ref_pattern = r'^\s*references\s+(\w+)\s*\((\w+)\)\s*$'
                match_ref = re.match(
                    ref_pattern, column_definition_match.group(3), re.IGNORECASE)

                w_pattern = r'^\s*$'
                match_w = re.match(
                    w_pattern, column_definition_match.group(3), re.IGNORECASE)

                if match_pk != None:
                    if len(primary_keys) > 0:
                        return parse_handle_invalid_syntax_for_creating_table()
                    else:
                        primary_keys.append(column_name)

                if match_ref != None:
                    ref_table_name = match_ref.group(1)
                    ref_column_name = match_ref.group(2)
                    references.append(
                        (column_name, ref_table_name, ref_column_name))

                if match_ref == None and match_pk == None and match_w == None:
                    return parse_handle_invalid_syntax_for_creating_table()
            else:
                return parse_handle_invalid_syntax_for_creating_table()

        for column_name in composite_primary_keys:
            if column_name not in [column for (column, _) in column_definitions]:
                return parse_handle_invalid_syntax_for_creating_table()

        for column_name in composite_primary_keys:
            for (column, data_type_in_col) in column_definitions:
                if column == column_name:
                    data_type = data_type_in_col
                    break
            primary_keys.append((column_name, data_type))

        return {
            'code': 2,
            'type': 'create',
            'object_type': 'table',
            'table_name': table_name,
            'column_definitions': column_definitions,
            'primary_keys': primary_keys,
            'references': references
        }


def parse_handle_create_index(syntax_in_sql):
    create_index_pattern = r'^create\s+index\s+(\w+)\s+on\s+(\w+)\s*\((.*)\)\s*;?$'
    match = re.match(create_index_pattern, syntax_in_sql, flags=re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_creating_index()
    else:
        index_name = match.group(1)
        table_name = match.group(2)

        columns = []
        columns_str = match.group(3)
        for columns_str_splited in columns_str.split(','):
            column_match = re.match(r'\s*(\w+)\s*', columns_str_splited)
            if column_match is None:
                return parse_handle_invalid_syntax_for_inserting()
            else:
                column = column_match.group(1)
                columns.append(column)

        return {
            'code': 3,
            'type': 'create',
            'object_name': 'index',
            'index_name': index_name,
            'table_name': table_name,
            'columns': columns
        }


def parse_handle_drop_database(syntax_in_sql):
    drop_database_pattern = r'^drop\s+database\s+(\w+)\s*;?$'
    match = re.match(drop_database_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_dropping_database()
    else:
        database_name = match.group(1)
        return {
            'code': 4,
            'type': 'drop',
            'object_type': 'database',
            'database_name': database_name
        }


def parse_handle_drop_table(syntax_in_sql):
    drop_table_pattern = r'^drop\s+table\s+(\w+)\s*;?$'
    match = re.match(drop_table_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_dropping_table()
    else:
        table_name = match.group(1)
        return {
            'code': 5,
            'type': 'drop',
            'object_type': 'table',
            'table_name': table_name
        }


def parse_handle_use(syntax_in_sql):
    use_pattern = r'^use\s+(\w+)\s*;?$'
    match = re.match(use_pattern, syntax_in_sql, flags=re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_use()
    else:
        database_name = match.group(1)
        return {
            'code': 6,
            'type': 'use',
            'database_name': database_name
        }


def parse_handle_insert(syntax_in_sql):
    insert_pattern = r'^insert\s+into\s+(\w+)\s*\((.*)\)\s*values\s*\((.*)\)\s*;?$'
    match = re.match(insert_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_inserting()
    else:
        table_name = match.group(1)

        columns = []
        columns_str = match.group(2)
        for columns_str_splited in columns_str.split(','):
            column_match = re.match(r'\s*(\w+)\s*', columns_str_splited)
            if column_match is None:
                return parse_handle_invalid_syntax_for_inserting()
            else:
                column = column_match.group(1)
                columns.append(column)

        values = []
        values_str = match.group(3)
        for values_str_splited in values_str.split(','):
            value_match = re.match(r'\s*([\w\' \@\.\-\_]+)\s*', values_str_splited)
            if value_match is None:
                return parse_handle_invalid_syntax_for_inserting()
            else:
                value = value_match.group(1)
                values.append(value)

        if len(values) != len(columns):
            return parse_handle_invalid_syntax_for_inserting()

        return {
            'code': 7,
            'type': 'insert',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }


def parse_handle_delete(syntax_in_sql):
    delete_pattern = r'^delete\s+from\s+(\w+)\s+where\s+(.*)\s*;?$'
    match = re.match(delete_pattern, syntax_in_sql, re.IGNORECASE)

    if match is None:
        return parse_handle_invalid_syntax_for_deleting()
    else:
        table_name = match.group(1)
        condition_str = match.group(2)

        condition = parse_handle_condition(condition_str)
        return {
            'code': 8,
            'type': 'delete',
            'table_name': table_name,
            'condition': condition
        }


def parse(syntax_in_sql: str):
    syntax_in_sql_splited = re.findall(r'\w+|[^\w\s]', syntax_in_sql.upper())

    if len(syntax_in_sql_splited) == 0:
        return parse_handle_no_input()

    command_type = syntax_in_sql_splited[0]

    if command_type == 'CREATE':
        if len(syntax_in_sql_splited) == 1:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_create_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_create_table(syntax_in_sql)
        elif object_type == 'INDEX':
            return parse_handle_create_index(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()

    elif command_type == 'DROP':
        if len(syntax_in_sql_splited) < 3:
            return parse_handle_invalid_object_type()

        object_type = syntax_in_sql_splited[1]

        if object_type == 'DATABASE':
            return parse_handle_drop_database(syntax_in_sql)
        elif object_type == 'TABLE':
            return parse_handle_drop_table(syntax_in_sql)
        else:
            return parse_handle_invalid_object_type()
    elif command_type == 'USE':
        if len(syntax_in_sql_splited) < 2:
            return parse_handle_invalid_object_type()
        return parse_handle_use(syntax_in_sql)
    elif command_type == 'INSERT':
        return parse_handle_insert(syntax_in_sql)
    elif command_type == 'DELETE':
        return parse_handle_delete(syntax_in_sql)
    else:
        return parse_handle_invalid_sql_command()


def handle_my_sql_input(input_str: str):
    sql_code_without_comments = re.sub('/\*.*?\*/', '', input_str)

    commands_raw = re.split('(CREATE|INSERT|USE|DROP|DELETE)',
                            sql_code_without_comments, flags=re.IGNORECASE)

    commands_raw = [command_raw.strip()
                    for command_raw in commands_raw if command_raw.strip()]

    for i in range(len(commands_raw)):
        if commands_raw[i].upper() in ['CREATE', 'INSERT', 'USE', 'DROP', 'DELETE']:
            commands_raw[i] += ' ' + commands_raw[i + 1]
            commands_raw[i + 1] = ''

    commands_in_sql = [command_raw.strip()
                       for command_raw in commands_raw if command_raw.strip()]

    commands = []
    for command_in_sql in commands_in_sql:
        command = parse(command_in_sql)
        commands.append(command)

    return commands


input = """
create database University;

USE University;

CREATE TABLE disciplines (
  DiscID varchar(5) PRIMARY KEY,
  DName varchar(30),
  CreditNr int
);


CREATE INDEX MixedIndex on disciplines (DiscID, CreditNr);


/*Data for the table disciplines */

insert into disciplines (DiscID,DName,CreditNr) values ('DB1','Databases 1', 7);
insert into disciplines (DiscID,DName,CreditNr) values ('DS','Data Structures',6);
insert into disciplines (DiscID,DName,CreditNr) values ('CP','C Programming',8);
insert into disciplines (DiscID,DName,CreditNr) values ('ST','Statistics',5);
insert into disciplines (DiscID,DName,CreditNr) values ('LT','Lattice Theory',8);
insert into disciplines (DiscID,DName,CreditNr) values ('OOP','Object Oriented Programming',6);
insert into disciplines (DiscID,DName,CreditNr) values ('AI','Artificial Intelligence',8);
insert into disciplines (DiscID,DName,CreditNr) values ('OS','Operating Systems',3);
insert into disciplines (DiscID,DName,CreditNr) values ('DB2','Databases 2',6);
insert into disciplines (DiscID,DName,CreditNr) values ('MA','Math Analysis',7);
insert into disciplines (DiscID,DName,CreditNr) values ('SI','Software Engineering',6);
insert into disciplines (DiscID,DName,CreditNr) values ('AL','Algebra',4);


/*Table structure for table specialization */


CREATE TABLE specialization (
  SpecID varchar(5) PRIMARY KEY,
  SpecName varchar(40),
  Language varchar(20) 
);

/*Data for the table specialization */

insert into specialization (SpecID,SpecName,Language) values ('I','Informatics','English');
insert into specialization (SpecID,SpecName,Language) values ('MI','Mathematics Informatics','English');
insert into specialization (SpecID,SpecName,Language) values ('M','Mathematics','English');
insert into specialization (SpecID,SpecName,Language) values ('P','Physics','German');
insert into specialization (SpecID,SpecName,Language) values ('CM','Computational Mathematics','German');
insert into specialization (SpecID,SpecName,Language) values ('A','Automatization','French');
insert into specialization (SpecID,SpecName,Language) values ('ING','Engineering','French');
insert into specialization (SpecID,SpecName,Language) values ('C','Calculators','French');
insert into specialization (SpecID,SpecName,Language) values ('MP','Mathematics and Physics','German');


/*Table structure for table groups */

CREATE TABLE groups (
  GroupId int PRIMARY KEY,
  SpecID varchar(20) REFERENCES specialization (SpecID)
);

/*Data for the table groups */

insert into groups (GroupId,SpecID) values (531,'I');
insert into groups (GroupId,SpecID) values (532,'I');
insert into groups (GroupId,SpecID) values (533,'I');
insert into groups (GroupId,SpecID) values (631,'MI');
insert into groups (GroupId,SpecID) values (632,'MI');
insert into groups (GroupId,SpecID) values (633,'MI');
insert into groups (GroupId,SpecID) values (431,'M');
insert into groups (GroupId,SpecID) values (432,'M');
insert into groups (GroupId,SpecID) values (931,'P');
insert into groups (GroupId,SpecID) values (451,'CM');
insert into groups (GroupId,SpecID) values (452,'CM');
insert into groups (GroupId,SpecID) values (731,'A');
insert into groups (GroupId,SpecID) values (732,'A');
insert into groups (GroupId,SpecID) values (831,'ING');
insert into groups (GroupId,SpecID) values (832,'ING');
insert into groups (GroupId,SpecID) values (833,'ING');
insert into groups (GroupId,SpecID) values (834,'ING');
insert into groups (GroupId,SpecID) values (331,'C');
insert into groups (GroupId,SpecID) values (332,'C');
insert into groups (GroupId,SpecID) values (231,'MP');
insert into groups (GroupId,SpecID) values (232,'MP');
insert into groups (GroupId,SpecID) values (233,'MP');

/*Table structure for table students */

CREATE TABLE students (
  StudID int PRIMARY KEY,
  GroupId int REFERENCES groups (GroupID),
  StudName varchar(20),
  Email varchar(20) 
);

/*Data for the table students */

insert into students (StudID,GroupId,StudName,Email) values (1,531,'John Foreman','JohnForeman@email.co');
insert into students (StudID,GroupId,StudName,Email) values (2,531,'Ashley Cole','Ashley Cole@email.co');
insert into students (StudID,GroupId,StudName,Email) values (3,531,'David Lineker','David Lineker@email.');
insert into students (StudID,GroupId,StudName,Email) values (4,531,'John Smith','JohnSmith@email.com');
insert into students (StudID,GroupId,StudName,Email) values (5,533,'Robert White','RobertWhite@email.co');
insert into students (StudID,GroupId,StudName,Email) values (6,533,'Monica Tyson','MonicaTyson@email.co');
insert into students (StudID,GroupId,StudName,Email) values (7,533,'Eva Blum','EvaBlum@email.com');
insert into students (StudID,GroupId,StudName,Email) values (8,532,'Nicolas Pitt','NicolasPitt@email.co');
insert into students (StudID,GroupId,StudName,Email) values (9,631,'Richard Roberts','RichardRoberts@email');
insert into students (StudID,GroupId,StudName,Email) values (11,631,'Julia Gere','JuliaGere@email.com');
insert into students (StudID,GroupId,StudName,Email) values (12,631,'Paul Smith','PaulSmith@email.com');
insert into students (StudID,GroupId,StudName,Email) values (13,632,'Paul Quimby','PaulQuimby@email.com');
insert into students (StudID,GroupId,StudName,Email) values (14,632,'Jack Curtis','JackCurtis@email.com');
insert into students (StudID,GroupId,StudName,Email) values (15,632,'Jennifer Ant','JenniferAnt@email.co');
insert into students (StudID,GroupId,StudName,Email) values (16,431,'Marc Alda','MarcAlda@email.com');
insert into students (StudID,GroupId,StudName,Email) values (17,432,'Orson Bauer','OrsonBauer@email.com');
insert into students (StudID,GroupId,StudName,Email) values (18,432,'Irving Benatar','IrvingBenatar@email.');
insert into students (StudID,GroupId,StudName,Email) values (19,432,'Garth Brooks','GarthBrooks@email.co');
insert into students (StudID,GroupId,StudName,Email) values (20,931,'Irene Cates','IreneCates@email.com');
insert into students (StudID,GroupId,StudName,Email) values (21,931,'Julia Chase','JuliaChase@email.com');
insert into students (StudID,GroupId,StudName,Email) values (22,931,'Cyd Child','CydChild@email.com');
insert into students (StudID,GroupId,StudName,Email) values (23,931,'Ashley Cole','AshleyCole2@email.co');
insert into students (StudID,GroupId,StudName,Email) values (24,451,'Miley Divine','MileyDivine@email.co');
insert into students (StudID,GroupId,StudName,Email) values (25,451,'Julia Derek','JuliaDerek@email.com');
insert into students (StudID,GroupId,StudName,Email) values (26,451,'Susan Denver','SusanDenver@email.co');
insert into students (StudID,GroupId,StudName,Email) values (27,451,'Jean Harrison','JeanHarrison@email.c');
insert into students (StudID,GroupId,StudName,Email) values (28,731,'Judy Hudson','JudyHudson@email.com');
insert into students (StudID,GroupId,StudName,Email) values (29,731,'Monica Han','MonicaHan@email.com');
insert into students (StudID,GroupId,StudName,Email) values (30,732,'Maria Jillian','MariaJillian@email.c');
insert into students (StudID,GroupId,StudName,Email) values (31,732,'Jay Lewis','JayLewis@email.com');
insert into students (StudID,GroupId,StudName,Email) values (32,831,'Chuck Ocean','ChuckOcean@email.com');
insert into students (StudID,GroupId,StudName,Email) values (33,831,'Frank Orlando','FrankOrlando@email.c');
insert into students (StudID,GroupId,StudName,Email) values (34,831,'Luke Polanski','LukePolanski@email.c');
insert into students (StudID,GroupId,StudName,Email) values (35,833,'River Pop','RiverPop@email.com');
insert into students (StudID,GroupId,StudName,Email) values (36,833,'Natalie Phoenix','NataliePhoenix@email');
insert into students (StudID,GroupId,StudName,Email) values (37,833,'Jane Rooney','JaneRooney@email.com');
insert into students (StudID,GroupId,StudName,Email) values (38,832,'Theresa Rule','TheresaRule@email.co');
insert into students (StudID,GroupId,StudName,Email) values (39,832,'Susan Sarandon','SusanSarandon@email.');
insert into students (StudID,GroupId,StudName,Email) values (40,834,'George Ryder','GeorgeRyder@email.co');
insert into students (StudID,GroupId,StudName,Email) values (41,834,'Nina Simmons','NinaSimmons@email.co');
insert into students (StudID,GroupId,StudName,Email) values (42,834,'Donna Sutherland','DonnaSutherland@emai');
insert into students (StudID,GroupId,StudName,Email) values (43,331,'Tiffany Streisand','TiffanyStreisand@ema');
insert into students (StudID,GroupId,StudName,Email) values (44,331,'Steven Twain','StevenTwain@email.co');
insert into students (StudID,GroupId,StudName,Email) values (45,331,'Bobby VOx','BobbyVox@email.com');
insert into students (StudID,GroupId,StudName,Email) values (46,332,'Bono Buren','BonoBuren@email.com');
insert into students (StudID,GroupId,StudName,Email) values (47,332,'Burt Waters','BurtWaters@email.com');
insert into students (StudID,GroupId,StudName,Email) values (48,231,'Julia Wayne','JuliaWayne@email.com');
insert into students (StudID,GroupId,StudName,Email) values (49,232,'Jane Rooney','JaneRooney2@email.co');
insert into students (StudID,GroupId,StudName,Email) values (50,233,'Rosa White','RosaWhite@email.com');


/*Table structure for table marks */


CREATE TABLE marks (
  StudID int(10) REFERENCES students (StudID),
  DiscID varchar(20) REFERENCES disciplines (DiscID),
  Mark int,
  PRIMARY KEY (StudID,DiscID)
);

/*Data for the table marks */

insert into marks (StudID,DiscID,Mark) values (1,'DB1',10);
insert into marks (StudID,DiscID,Mark) values (1,'DS',9);
insert into marks (StudID,DiscID,Mark) values (2,'AI',9);
insert into marks (StudID,DiscID,Mark) values (2,'CP',9);
insert into marks (StudID,DiscID,Mark) values (3,'OOP',6);
insert into marks (StudID,DiscID,Mark) values (3,'ST',8);
insert into marks (StudID,DiscID,Mark) values (4,'AI',6);
insert into marks (StudID,DiscID,Mark) values (4,'LT',5);
insert into marks (StudID,DiscID,Mark) values (5,'DB2',7);
insert into marks (StudID,DiscID,Mark) values (5,'MA',8);
insert into marks (StudID,DiscID,Mark) values (5,'OS',4);
insert into marks (StudID,DiscID,Mark) values (6,'AL',6);
insert into marks (StudID,DiscID,Mark) values (6,'DB1',5);
insert into marks (StudID,DiscID,Mark) values (6,'SI',9);
insert into marks (StudID,DiscID,Mark) values (7,'CP',7);
insert into marks (StudID,DiscID,Mark) values (7,'DS',4);
insert into marks (StudID,DiscID,Mark) values (8,'LT',5);
insert into marks (StudID,DiscID,Mark) values (8,'OOP',8);
insert into marks (StudID,DiscID,Mark) values (8,'ST',8);
insert into marks (StudID,DiscID,Mark) values (9,'AI',5);
insert into marks (StudID,DiscID,Mark) values (9,'OS',8);
insert into marks (StudID,DiscID,Mark) values (11,'AL',10);
insert into marks (StudID,DiscID,Mark) values (11,'SI',10);
insert into marks (StudID,DiscID,Mark) values (12,'AI',10);
insert into marks (StudID,DiscID,Mark) values (12,'CP',10);
insert into marks (StudID,DiscID,Mark) values (12,'DB1',10);
insert into marks (StudID,DiscID,Mark) values (13,'DB1',9);
insert into marks (StudID,DiscID,Mark) values (13,'OS',9);
insert into marks (StudID,DiscID,Mark) values (14,'AL',9);
insert into marks (StudID,DiscID,Mark) values (14,'DB2',8);
insert into marks (StudID,DiscID,Mark) values (14,'MA',8);
insert into marks (StudID,DiscID,Mark) values (15,'AL',6);
insert into marks (StudID,DiscID,Mark) values (15,'SI',9);
insert into marks (StudID,DiscID,Mark) values (16,'CP',9);
insert into marks (StudID,DiscID,Mark) values (16,'OOP',8);
insert into marks (StudID,DiscID,Mark) values (17,'DB2',5);
insert into marks (StudID,DiscID,Mark) values (18,'DS',7);
insert into marks (StudID,DiscID,Mark) values (19,'AI',6);
insert into marks (StudID,DiscID,Mark) values (19,'LT',5);
insert into marks (StudID,DiscID,Mark) values (19,'MA',4);
insert into marks (StudID,DiscID,Mark) values (19,'OS',4);
insert into marks (StudID,DiscID,Mark) values (20,'DB1',4);
insert into marks (StudID,DiscID,Mark) values (20,'DB2',5);
insert into marks (StudID,DiscID,Mark) values (21,'AI',5);
insert into marks (StudID,DiscID,Mark) values (21,'SI',8);
insert into marks (StudID,DiscID,Mark) values (22,'ST',8);
insert into marks (StudID,DiscID,Mark) values (23,'AI',7);
insert into marks (StudID,DiscID,Mark) values (23,'LT',8);
insert into marks (StudID,DiscID,Mark) values (24,'OOP',7);
insert into marks (StudID,DiscID,Mark) values (25,'OS',7);
insert into marks (StudID,DiscID,Mark) values (26,'AL',8);
insert into marks (StudID,DiscID,Mark) values (26,'MA',4);
insert into marks (StudID,DiscID,Mark) values (27,'DS',5);
insert into marks (StudID,DiscID,Mark) values (27,'LT',9);
insert into marks (StudID,DiscID,Mark) values (27,'SI',8);
insert into marks (StudID,DiscID,Mark) values (28,'DS',4);
insert into marks (StudID,DiscID,Mark) values (29,'DS',9);
insert into marks (StudID,DiscID,Mark) values (30,'DS',6);
insert into marks (StudID,DiscID,Mark) values (31,'AI',5);
insert into marks (StudID,DiscID,Mark) values (31,'DS',7);
insert into marks (StudID,DiscID,Mark) values (31,'LT',6);
insert into marks (StudID,DiscID,Mark) values (32,'ST',8);
insert into marks (StudID,DiscID,Mark) values (33,'DB1',7);
insert into marks (StudID,DiscID,Mark) values (33,'DB2',8);
insert into marks (StudID,DiscID,Mark) values (33,'SI',9);
insert into marks (StudID,DiscID,Mark) values (34,'CP',8);
insert into marks (StudID,DiscID,Mark) values (34,'LT',4);
insert into marks (StudID,DiscID,Mark) values (34,'OOP',5);
insert into marks (StudID,DiscID,Mark) values (35,'OS',10);
insert into marks (StudID,DiscID,Mark) values (36,'SI',10);
insert into marks (StudID,DiscID,Mark) values (36,'ST',10);
insert into marks (StudID,DiscID,Mark) values (37,'DS',9);
insert into marks (StudID,DiscID,Mark) values (38,'CP',9);
insert into marks (StudID,DiscID,Mark) values (39,'OOP',9);
insert into marks (StudID,DiscID,Mark) values (40,'DS',10);
insert into marks (StudID,DiscID,Mark) values (40,'ST',9);
insert into marks (StudID,DiscID,Mark) values (41,'AL',5);
insert into marks (StudID,DiscID,Mark) values (41,'DB2',9);
insert into marks (StudID,DiscID,Mark) values (41,'MA',6);
insert into marks (StudID,DiscID,Mark) values (43,'LT',8);
insert into marks (StudID,DiscID,Mark) values (44,'OOP',6);
insert into marks (StudID,DiscID,Mark) values (45,'DS',7);
insert into marks (StudID,DiscID,Mark) values (45,'OS',5);
insert into marks (StudID,DiscID,Mark) values (46,'AI',4);
insert into marks (StudID,DiscID,Mark) values (46,'SI',8);
insert into marks (StudID,DiscID,Mark) values (47,'ST',8);
insert into marks (StudID,DiscID,Mark) values (48,'MA',9);
insert into marks (StudID,DiscID,Mark) values (49,'AL',6);
insert into marks (StudID,DiscID,Mark) values (50,'AI',7);
insert into marks (StudID,DiscID,Mark) values (50,'OOP',10);
insert into marks (StudID,DiscID,Mark) values (50,'OS',5);
"""

asd = handle_my_sql_input(input)
for i in asd:
    print(i, end='\n')
