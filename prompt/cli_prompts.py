from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter, Completer, Completion
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer
from prompt_toolkit.styles import Style
from prompt_toolkit.history import FileHistory

import re

class SqlPositionalCompleter(Completer):
    def __init__(self, databases, tables_in_database: list[str], tables_with_columns_in_database: list[str]):
        self.keywords = [
            'use', 'create', 'drop', 'insert', 'into', 'values', 'delete', 'from', 'where',
            'select', 'join', 'on', 'group', 'by', 'update', 'primary', 'key', 'references',
            'unique', 'as', 'not', 'and', 'or', 'like', 'database', 'table', 'index',
        ]

        self.type_keywords = [
            'int', 'varchar', 'bit', 'date', 'datetime', 'float',
        ]

        self.function_keywords = [
            'avg()', 'min()', 'max()', 'count()', 'sum()',
        ]

        self.databases = databases
        self.tables = tables_in_database
        self.tables_with_columns = tables_with_columns_in_database

    def get_completions(self, document, complete_event):
        word_before_cursor = document.get_word_before_cursor()
        
        prompt_words = document.text_before_cursor.split()
        prompt_words_count = len(prompt_words)

        prompt_chars = document.text_before_cursor

        #> GENERAL TABLE AND COLUMN SUGGESTION ONLY WHEN TYPING <#

        additional_suggestions = []
        if prompt_words_count >= 1 and prompt_chars[-1] != ' ':
            for table in self.tables:
                if table.lower().startswith(word_before_cursor.lower()):
                    additional_suggestions.append(table)
                for column in self.tables_with_columns[table]:
                    if column.lower().startswith(word_before_cursor.lower()):
                        additional_suggestions.append(column)

        #> GENERAL FUNCTION SUGGESTION ONLY WHEN TYPING <#

        function_suggestions = []
        if prompt_words_count >= 1 and prompt_chars[-1] != ' ':
            for function in self.function_keywords:
                if function.lower().startswith(word_before_cursor.lower()):
                    function_suggestions.append(function)

        #> GENERAL TYPE SUGGESTIONS ONLY WHEN TYPING <#

        type_suggestions = []
        if prompt_words_count >= 1 and prompt_chars[-1] != ' ':
            for type in self.type_keywords:
                if type.lower().startswith(word_before_cursor.lower()):
                    type_suggestions.append(type)
        

        if document.find_backwards('(') and not document.find_backwards(')'):
            create_table_keywords = ['primary', 'key', 'unique', 'references']
            
            suggestions = []
            if prompt_words_count >= 1 and prompt_chars[-1] != ' ':
                for keyword in create_table_keywords:
                    if keyword.lower().startswith(word_before_cursor.lower()):
                        suggestions.append(keyword)

            for suggestion in suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            for suggestion in type_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansigreen italic')
            return
        
        if len(document.find_all('\''))%2 != 0:
            return
                
        #> NO SUGGESTION AFTER KEYWORD <#
        
        #> create database _
        if prompt_words_count >= 3 and prompt_words[-3].lower() == 'create' and prompt_words[-2] == 'database':
            return
        
        #> create table _
        if prompt_words_count >= 3 and prompt_words[-3].lower() == 'create' and prompt_words[-2] == 'table':
            return

        #> as _
        if prompt_words_count >= 2 and prompt_words[-2].lower() == 'as':
            return

        #> DATABASE SUGGESTION AFTER USE <#

        if prompt_words_count >= 2 and prompt_words[-2].lower() == 'use' and prompt_chars[-1] != ' ':
            suggestions = []
            for database in self.databases:
                if database.lower().startswith(prompt_words[-1].lower()):
                    suggestions.append(database)
            for suggestion in suggestions:
                yield Completion(text=suggestion, start_position=document.find_previous_word_beginning(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            return

        #> KEYWORD SUGGESTION AFTER KEYWORD <#

        #> insert into
        if prompt_words_count >= 1 and prompt_words[-1].lower() == 'insert' and prompt_chars[-1] == ' ':
            suggestions = ['into']
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            return

        #> delete from
        if prompt_words_count >= 1 and prompt_words[-1].lower() == 'delete' and prompt_chars[-1] == ' ':
            suggestions = ['from']
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            return

        #> primary key
        if prompt_words_count >= 1 and prompt_words[-1].lower() == 'primary' and prompt_chars[-1] == ' ':
            suggestions = ['key']
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            return
        
        #> create [database, table, index]
        if prompt_words_count >= 1 and prompt_words[-1].lower() == 'create' and prompt_chars[-1] == ' ':
            suggestions = ['database', 'table', 'index']
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            return
        
        #> drop [database, table]
        if prompt_words_count >= 1 and prompt_words[-1].lower() == 'drop' and prompt_chars[-1] == ' ':
            suggestions = ['database', 'table']
            for suggestion in suggestions:
                yield Completion(text=suggestion, style='bg:#2B2B37 fg:blue bold')
            return

        #> [int, bit, float, varchar(), date, datetime] [primary, unique, references]
        if prompt_words_count >= 1 and prompt_words[-1].lower() in self.type_keywords and prompt_chars[-1] == ' ':
            suggestions = ['primary', 'unique', 'references']
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')
            return

        #> COLUMN SUGGESTION AFTER '.' CHARACTER <#

        if prompt_chars and prompt_chars[-1] == '.':
            suggestions = []
            table_name = prompt_words[-1].strip('.')
            
            if table_name in self.tables:
                suggestions = self.tables_with_columns.get(table_name, [])
            
            for suggestion in suggestions:
                yield Completion(text=suggestion, selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')

            return

        if prompt_words_count >= 1 and prompt_words[-1].find('.') != -1 and prompt_chars[-1] != ' ' and prompt_chars[-1] != '.':
            suggestions = []
            table_name = prompt_words[-1].split('.')[0]
            if table_name in self.tables:
                columns = self.tables_with_columns.get(table_name, [])
                for column in columns:
                    if column.startswith(prompt_words[-1].split('.')[1]):
                        suggestions.append(column)

            for suggestion in suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            return

        #> TABLE SUGGESTION AFTER KEYWORD ONLY WHEN TYPING <#

        #> [from, join, into, on] _table_
        if prompt_words_count >= 2 and prompt_words[-2].lower() in ['from', 'into', 'join', 'on'] and prompt_chars[-1] != ' ':
            suggestions = []
            for table in self.tables:
                if table.lower().startswith(word_before_cursor.lower()):
                    suggestions.append(table)

            for suggestion in suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            return 

        #> TABLE OR COLUMN SUGGESTION AFTER KEYWORD ONLY WHEN TYPING <#
        
        #> [select, where] [table|column|function] 
        if prompt_words_count >= 2 and prompt_words[-2].lower() in ['select', 'where'] and prompt_chars[-1] != ' ':
            for suggestion in additional_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            
            for suggestion in function_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue italic bold')
            return

        # #> [',', <, >, <>, <=, >=, =] [table|column]
        # match = re.match(r'^\s*(.*?)$\s*(<>|>=|<=|=|>|<|,)\s*')
        def matches_op(string):
            pattern = r'^\s*(.*?)(<>|>=|<=|=|>|<|,)\s*$'
            if re.match(pattern, string,flags=re.IGNORECASE) != None : return True
            else: return False
        
        if prompt_words_count >= 2 and matches_op(prompt_words[-2]):
            for suggestion in additional_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            
            for suggestion in function_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue italic bold')
            return
        
        if len(prompt_chars) >=2 and prompt_chars[-2] in ['=', '>', '<', ',']:
            for suggestion in additional_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')
            
            for suggestion in function_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue italic bold')
            return
        

        # No specific context, provide general suggestions
        if prompt_chars and prompt_chars[-1] != ' ':
            suggestions = []

            for keyword in self.keywords:
                if keyword.startswith(word_before_cursor.lower()):
                    suggestions.append(keyword)

            for suggestion in suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue bold')

            for suggestion in additional_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:white italic')

            for suggestion in type_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansigreen italic')

            for suggestion in function_suggestions:
                yield Completion(text=suggestion, start_position=document.find_start_of_previous_word(), selected_style='bg:grey', style='bg:#2B2B37 fg:ansiblue italic bold')
            return

        return


CLI_PROMPT_HISTORY_FILE = 'prompt/cli_prompt_history'

SQL_PROMPT_HISTORY_FILE = 'prompt/sql_prompt_history'


def sql_prompt(key_word_buffer, databases, tables_in_database, tables_with_columns_in_database):
    SQL_HISTORY = FileHistory(SQL_PROMPT_HISTORY_FILE)

    prompt_style = Style.from_dict({
        'console': 'ansiblue italic',
        'pygments.keyword': 'ansiblue italic'
    })

    prompt_message = [
        ('class:console', '> ')
    ]

    return prompt(
        prompt_message,
        style=prompt_style,

        completer=SqlPositionalCompleter(
            databases, tables_in_database, tables_with_columns_in_database),
        complete_while_typing=True,

        lexer=PygmentsLexer(SqlLexer),

        history=SQL_HISTORY
    )


def cli_prompt():
    CLI_KEYWORD_COMPLETER = WordCompleter([
        'run',
        'connect',
        'tree',
        'commands',
        'exit',
        'console'
    ], ignore_case=True)

    CLI_HISTORY = FileHistory(CLI_PROMPT_HISTORY_FILE)

    prompt_style = Style.from_dict({
        'input': '#3cb371 italic'
    })

    prompt_message = [
        ('class:input', '[Input]: ')
    ]

    return prompt(
        prompt_message,
        style=prompt_style,

        completer=CLI_KEYWORD_COMPLETER,
        complete_while_typing=True,
        complete_style=CompleteStyle.READLINE_LIKE,

        history=CLI_HISTORY
    )



