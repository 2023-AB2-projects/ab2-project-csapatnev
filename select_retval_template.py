# JELMAGYARAZAT:
# [] -> lista
# a | b -> lehet <a> is es <b> is

select_clause = [{
    'table_name': str | 'unknown', 
    'column_name': str, 
    'alias': str, 
    'function': 'avg' | 'count' | 'min' | 'max' | 'sum' | None
}] # -> lista ilyen elemekkel 

join_clause = [{
    'table': str,                                        # join-olt tabla
    'lsh': {'table_name': str, 'column_name': str},      # az egyenloseg bal oldala
    'rhs': {'table_name': str, 'column_name': str}       # az egyenloseg jobb oldala
}] # -> lista ilyen elemekkel

where_clause = [{
    'negation': True | False,
    'operator': "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$ne" | "$regex" | "$ne",
    'lhs': {                                            # az egyenloseg bal oldala
        'type': 'column',
        'table_name': str | 'unknown',
        'column_name': str
    },
    'rhs': {                                            # az egyenloseg jobb oldala
        'type': 'column',
        'table_name': str | 'unknown',
        'column_name': str
    } | {
        'type': 'string',
        'value': str
    } | {
        'type': 'number',
        'value': str
    }
}] # -> lista ilyen elemekkel

groupby_clause = [{
    'table_name': str | 'unknown', 
    'column_name': str
}] # -> lista ilyen elemekkel

# EZ A VEGSO VISSZATERITESI SABLON (A TOBBI CSAK EGY EGY RESZE):
retval = {
    'code': 10,
    'type': 'select',
    'select_clause': select_clause,
    'select_disticnt': True | False,
    'from_clause': str,
    'join_clause': join_clause,
    'where_clause': where_clause,
    'groupby_clause': groupby_clause
}