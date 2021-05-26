import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'

# List of rows to insert into the DB
sql_transaction = []

#  Create DB called the month of the data and connect to it
connection = sqlite3.connect('{}.db'.format(timeframe))

# Create cursor object
c = connection.cursor()

def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

# Normalize the data - "newlinechar" will be used to get rid of the new line character and separate words with new line in between
def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', '"')
    return data      

def find_parent(pid):
    try:
        query = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(query)
        result = c.fetchone()
        if result != None:
            return result[0] # Returns list
        else: 
            return False
    except Exception as e:
        return False

def find_existing_score(pid):
    try:
        query = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(query)
        result = c.fetchone()
        if result != None:
            return result[0] # Returns list
        else: 
            return False
    except Exception as e:
        return False    

# Find if data is acceptable, not acceptable if:
# - More than 50 words or less than 1 character
# - More than 1000 characters
# - Comment was deleted or removed
def is_acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True 

def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))


def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))

# Insert rows in blocks of 1000
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except: 
                pass
        connection.commit()
        sql_transaction = []    

if __name__ == '__main__':
    create_table()
    # How many rows have we iterated through
    row_counter = 0
    # How many parent-child pairs we have, needed because some comments won't have a reply child
    paired_rows = 0

    # Use burffering in order to parse throught the file in chunks 
    with open('RC_{}'.format(timeframe), buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']

            # Get parent body text
            parent_data = find_parent(parent_id)

            # Since we work in pairs, we want to find the highest score reply of a parent comment,
            # If there's already one, replace it 
            if score >= 2:
                if is_acceptable(body):
                    # Find score of the reply
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        # If current reply score is bigger than the one stored, replace the reply comment 
                        if score > existing_comment_score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    # If there isn't a reply     
                    else:
                        # If there's a parent then insert as reply
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        # If there's no parent then insert as parent
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows, str(datetime.now())))