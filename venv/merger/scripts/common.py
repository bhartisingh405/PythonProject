import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import ast
import json
from dateutil.parser import parse
import dateutil
import numpy as np


def postgresql_to_record(conn, select_query):
    cursorInner = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursorInner.execute(select_query)
        dictRows = cursorInner.fetchall()
        cursorInner.close()
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    return dictRows


def postgresql_to_row_count(conn, select_query):
    cursorInner = conn.cursor()
    try:
        cursorInner.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    data = cursorInner.fetchone()
    cursorInner.close()
    return data[0]


def postgresql_to_dataframe(conn, select_query, columns):
    cursorInner = conn.cursor()
    try:
        cursorInner.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as errors:
        print("Error: %s" % errors)
        cursorInner.close()
        return 1
    tuples = cursorInner.fetchall()
    cursorInner.close()
    df_inner = pd.DataFrame(tuples, columns=columns)
    return df_inner


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def add_quotes(param):
    dateformat = '%Y-%m-%d %H:%M:%S'
    if param is None or param is np.nan or param == '':
        return 'null'
    elif is_number(param) or param.isdigit() or param.isnumeric() or param.isdecimal():
        return ast.literal_eval(param)
    elif is_date(param):
        date_str = dateutil.parser.parse(param)
        return "\'" + date_str.strftime(dateformat) + "\'"
    elif is_dict(param):
        return "\'" + json.dumps(ast.literal_eval(param)) + "\'"
    elif isinstance(param, str):
        return "\'" + param + "\'"
    else:
        return param


def is_date(string, fuzzy=False):
    try:
        parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False


def is_dict(string):
    try:
        result = ast.literal_eval(string)
        return isinstance(result, dict)
    except (SyntaxError, ValueError):
        return False


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True
