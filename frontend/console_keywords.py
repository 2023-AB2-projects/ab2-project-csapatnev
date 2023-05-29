# returns all possible sql keywords
def get_sql_keywords():
    return [
        # sql commands:
        'use',
        'create',
        'drop',
        'insert',
        'into',
        'values',
        'delete',
        'from',
        'where',
        'select',
        'from',
        'join',
        'on',
        'group',
        'by',
        'update',
        'primary',
        'key',
        'references',
        'unique',
        'as',
        'not',
        'and',
        'or',
        'like'
        # sql objects:
        'database',
        'table',
        'index',
        # sql types:
        'int',
        'varchar',
        'bit',
        'date',
        'datetime',
        'float'
    ]


def get_sql_function_keywords():
    return [
        'avg()',
        'min()',
        'max()',
        'count()',
        'sum()',
    ]


# def get_
