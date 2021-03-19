from mysql.connector import MySQLConnection, Error
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, parse_qsl
from os import path
import json, time, sys, datetime, re

port = 3000

# read_db_config from specified path
def read_db_config(filename='./config/mysql.json'):
    if(path.exists(filename)): # path valid
        with open(filename) as f:
            try:
                data = json.load(f)
                db = {}
                db['host'] = data['host']
                db['port'] = data['port']
                db['user'] = data['user']
                db['password'] = data['pass']
                db['database'] = data['db']
                
                return db
            except:
                print('Not Json format')
                sys.exit(2)
    else:
        print('file not exist')
        sys.exit(2)



# test db connection
def connect():
    db_config = read_db_config()
    conn = None
    try:
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config)

        if conn.is_connected():
            print('Connection established.')
        else:
            print('Connection failed.')
            sys.exit(4)

    except Error as error:
        print(error)
        sys.exit(4)
    
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            print('Connection closed.')

#connect()

def is_valid_fname(fname):
    if fname.isalpha() and len(fname) <= 20: return fname
    return None

def is_valid_lname(lname):
    if lname == '' or lname == None:
        return True
    return bool(re.match(r'^[A-Za-z]+$', lname))

def is_valid_handed(handed):
    if handed == 'left' or handed == 'right' or handed == 'ambi': return handed
    return None

def is_valid_currency(currency):
    if currency == None: 
        return None
    dec = 0
    # check digit
    for i in range(0, len(currency)):
        if currency[i] == '.':
            dec = i + 1
            break
    if dec != 0:
        if len(currency) - dec > 2: 
            return None
    try:
        if float(currency) >= 0:
            return True
    except:
        return None

def is_valid_id(id):
    try:
        if id.isdigit() and int(id) > 0:
            return id
    except:
        return None

def is_valid_points(val):
    try:
        if int(val) > 0:
            return True
        else:
             return False
    except:
        return False
    
def to_boolean(tmp):
    if tmp == None:
        return None
    tmp = tmp.lower()
    if tmp == '1' or tmp == 'true' or tmp == 't':
        return True
    else:
        return False

## format_body helper

def format_body_name(fname, lname):
    if lname == None or lname == '':
        return fname
    return fname + " " + lname

def format_body_handed(handed):
    if handed == 'L': return "left"
    elif handed == 'R': return "right"
    else: return "ambi"

def format_body_currency(val):
    return f'{val:2f}'

#### Request
### Get Request
## handle GET /ping
def req_ping():
    http_code = 204
    body = ''
    return http_code, body

def req_404():
    http_code = 404
    body = ''
    return http_code, body

def req_player_list():
    row = load_player()
    http_code = 200
    return http_code, format_body_player(list(row))

def req_clash_list():
    print("in clash_list")
    rows = load_clash()
    http_code = 200
    return http_code, format_body_clash(list(rows))

def req_player_get(pid):
    print("--IN GET: PLAYER GET PID--")
    if not is_player_exist(pid):
        return 404, ''
    row = load_player(pid)
    http_code = 200
    return http_code, format_body_player(row)

def req_clash_get(cid):
    if not is_clash_exist(cid):
        print("-------")
        return 404, ''
    row = load_clash(cid)
    http_code = 200
    return http_code, format_body_clash(row)

### POST Request
# helper function for creating player
def req_admin_pre():

    for q in EXTEND_SCHEMA_SQL:
        cursor.execute(q)
    db.commit()
    
    http_code = 200
    body = 'OK'

    return http_code, body

####### Helper functions with SQL query
def create_player(fname, lname, handed, initial_balance_usd):
    query = "INSERT INTO player(fname,lname,handed,is_active,balance_usd)" \
            "VALUES(%s,%s,%s,%s,%s)"
    is_active = True
    args = (fname, lname, handed, is_active, initial_balance_usd)
    cursor.execute(query,args)
    db.commit()

def create_clash(pid1, pid2, entry_fee_usd, prize_usd):
    query = "INSERT INTO clash(player1_id,player2_id,entry_fee_usd,prize_usd,create_at)" \
            "VALUES(%s,%s,%s,%s,%s)"
    
    now_time = str(datetime.datetime.utcnow())
    args = (pid1, pid2, entry_fee_usd, prize_usd, now_time)
    cursor.execute(query,args)
    db.commit()

def update_player(pid, lname, is_active):
    query = "UPDATE player SET lname = %s, is_active = %s WHERE player_id = %s"
    args = (lname, is_active, int(pid))
    cursor.execute(query,args)
    db.commit()
    
def is_player_exist(pid):
    cursor.execute('SELECT * FROM player')
    rows = sql_rows_dict(cursor)
    for row in rows:
        if row['player_id'] == int(pid):
            print("player exist: pid =",pid)
            return True
    print("player not exist: pid =",pid)
    return False

def is_clash_exist(cid):
    cursor.execute('SELECT * FROM view_clash')
    rows = sql_rows_dict(cursor)
    for row in rows:
        if row['clash_id'] == int(cid):
            print("clash:", cid, "exist")
            return True
    return False

def is_clash_could_end(cid): # check active and point tie # need test -- sql syntax
    cursor.execute("SELECT * FROM view_clash")
    rows = sql_rows_dict(cursor)
    
    for row in rows:
        if row['clash_id'] == cid:
            if row['end_at'] is not None:
                print("(POST) clash_end: Clash not end!")
                return False
            elif row['p1_points'] == row['p2_points']:
                print("POST) clash_end: point tied!")
                return False
    
    return True

def is_clash_end(cid): # need test
    q = 'SELECT * FROM view_clash WHERE clash_id = %s' %(cid)
    cursor.execute(q)
    #cursor.execute("SELECT * FROM view_clash")
    row = sql_row_dict(cursor)
    if row['is_active'] == 1:
        return False
    return True

def is_player_in_active_clash(pid):

    cursor.execute('SELECT * FROM view_player_pre where player_id = %s' %pid)
    rows = sql_rows_dict(cursor)
    print(rows);
    for row in rows:
        if row['in_active_clash'] != None:
            print('pid:', pid,'is in_active_clash')
            return True
    return False

def is_player_in_clash(cid, pid):
    cursor.execute('SELECT * FROM clash where clash_id = %s' %cid)
    row = sql_row_dict(cursor)
    if int(pid) != row['player1_id'] and int(pid) != row['player2_id']:
        return False
    return True

def get_player_balance(pid):
    print('in get_player_balance')
    cursor.execute('SELECT * FROM view_player where player_id = %s' %pid)
    row = sql_row_dict(cursor)
    return float(row['balance_usd'])
##########################################################


## handle POST /player?fname=&lname=&handed=[enum]&initial_balance_usd=[currency]
def req_player_create(fname, lname, handed, initial_balance_usd):
    http_code = 303
    body = ''
    print('--IN POST: PLAYER CREATE--')
    create_player(fname, lname, handed, initial_balance_usd)
    re_path = '/player/' + str(cursor.lastrowid)
    header = {'Location': re_path} # still need pid after creating a player
    print(cursor.lastrowid)
    return http_code, body, header 

def req_clash_create(pid1, pid2, entry_fee_usd, prize_usd):
    if not is_player_exist(pid1) or not is_player_exist(pid2):
        return 404, ''
    elif is_player_in_active_clash(pid1) or is_player_in_active_clash(pid2):
        return 409, ''
    elif get_player_balance(pid1) < float(entry_fee_usd) or get_player_balance(pid2) < float(entry_fee_usd):
        return 402, ''

    print('--IN POST: CLASH CREATE--')
    query = "INSERT INTO clash(player1_id,player2_id,entry_fee_usd,prize_usd,create_at)" \
            "VALUES(%s,%s,%s,%s,%s)"
    
    now_time = str(datetime.datetime.utcnow())
    args = (pid1, pid2, entry_fee_usd, prize_usd, now_time)
    cursor.execute(query,args)
    cid = str(cursor.lastrowid)
    p1_bal = get_player_balance(pid1) - float(entry_fee_usd)
    p2_bal = get_player_balance(pid2) - float(entry_fee_usd)
    cursor.execute('UPDATE player SET balance_usd = %s WHERE player_id = %s' %(p1_bal,pid1))
    cursor.execute('UPDATE player SET balance_usd = %s WHERE player_id = %s' %(p2_bal,pid2))
    db.commit()
    http_code = 303
    re_path = '/clash/' + cid
    header = {'Location': re_path} # still need cid after creating a clash
    body = ''
    
    return http_code, body, header

def req_player_update(pid, lname, is_active): ## need to handler redirect
    print('--IN POST: UPDATE PLAYER--')
    if not is_player_exist(pid):
        return 404, ''
    
    if lname == None and is_active != None:
        query = '''UPDATE player SET is_active = %s WHERE player_id = %s'''
        args = (is_active, pid)
        cursor.execute(query, args)
        db.commit()
    elif lname != None and is_active == None:
        query = '''UPDATE player SET lname = %s WHERE player_id = %s'''
        args = (lname, pid)
        cursor.execute(query, args)
        db.commit()
    http_code = 303
    re_path = '/player/' + str(pid)
    header = {'Location': re_path}
    body = ''
    return http_code, body, header

def req_player_deposit(pid, amount_usd):
    if not is_player_exist(pid):
        print("Error: Deposit: Player Not Exist\n")
        return 404, ''
    http_code = 200
    cursor.execute("SELECT * FROM player WHERE player_id = %s" %(pid))
    row = sql_row_dict(cursor)
    old_bal = row['balance_usd']
    query = "UPDATE player SET balance_usd = %s + %s WHERE player_id = %s"
    args = (old_bal,amount_usd, pid)
    cursor.execute(query,args)
    cursor.execute("SELECT * FROM player WHERE player_id = %s" %(pid))
    row = sql_row_dict(cursor)
    new_bal = row['balance_usd']
    db.commit()
    body = {
        "old_balance_usd": str(old_bal),
        "new_balance_usd": str(new_bal)
    }
    return http_code, body

def req_clash_end(cid):
    if not is_clash_exist(cid):
        return 404, ''
    elif not is_clash_could_end(cid):
        return 409, ''
    now_time = str(datetime.datetime.utcnow())
    cursor.execute("UPDATE clash SET end_at = %s WHERE clash_id = %s", (now_time, int(cid)))
    db.commit()
    return req_clash_get(cid)

def req_clash_dq(cid, pid):
    if not is_clash_exist(cid) or not is_player_exist(pid):
        return 404, ''
    elif is_clash_end(cid):
        return 409, ''
    else:
        cursor.execute("SELECT * FROM view_clash")
        rows = sql_rows_dict(cursor)
        for row in rows:
            if row['clash_id'] == cid: # player not in the clash
                if row['player1_id'] != pid and row['player2_id'] != pid:
                    return 400, ''
        
        now_time = str(datetime.datetime.utcnow())
        is_dq = True #default

        ## Insert value(is_dq and event_at) into clash_point
        insert_query = "INSERT INTO clash_point(player_id,clash_id,is_dq,event_at)" \
                       "VALUES(%s,%s,%s,%s)"
        insert_args = (pid,cid,is_dq,now_time)

        ## Update clash(end_at)
        update_query= 'UPDATE clash SET end_at = NOW() WHERE clash_id = %s' %(cid)

        cursor.execute(insert_query,insert_args)
        cursor.execute(update_query)

        db.commit()
        return req_clash_get(cid)

def req_clash_award(cid, pid, points):
    print("44")
    if not is_clash_exist(cid) or not is_player_exist(pid) or not is_player_in_clash(cid,pid):
        return 404, ''
    print("33")
    if is_clash_end(cid):
        return 409, ''
    
    print("22")
    cursor.execute("SELECT * FROM view_clash")
    rows = sql_rows_dict(cursor)
    for row in rows:
        if row['clash_id'] == cid:
            if row['player1_id'] != pid and row['player2_id'] != pid:
                return 404, ''
        
    query = "INSERT INTO clash_point(player_id,clash_id,points,is_dq,event_at)" \
            "VALUES(%s,%s,%s,%s,%s)"
    now_time = str(datetime.datetime.utcnow())
    is_dq = False #default
    args = (pid,cid,points,is_dq,now_time)
    cursor.execute(query,args)
    db.commit()
    return req_clash_get(cid)
    
## Some other helper function
def sql_rows_dict(cursor):
    cols = tuple( [d[0] for d in cursor.description])
    rows = cursor.fetchall()

    return [dict(zip(cols, row)) for row in rows]

def sql_row_dict(cursor):
    cols = tuple( [d[0] for d in cursor.description])
    row = cursor.fetchone()

    return dict(zip(cols, row))

def load_player(pid = None):
    print("Load_Player: pid =", pid)
    if pid is not None:
        q = 'SELECT * FROM view_player WHERE player_id = %s' %(pid)
    else:
        q = 'SELECT * FROM view_player WHERE is_active = True'
    
    cursor.execute(q)
    rows = sql_rows_dict(cursor)
    #print("player list: ", rows)

    if pid is not None and len(rows) == 0:
        return None
    if pid is not None:
        return rows.pop()
    print("--")
    return rows

def load_clash(cid = None):
    print("Load_clash: cid=", cid)
    if cid is not None:
        q = 'SELECT * FROM view_clash where clash_id = %s' %(cid)
    else:
        q = 'SELECT * FROM view_clash where is_active = 1 ORDER BY prize_usd DESC'
        cursor.execute(q)
        rows1 = sql_rows_dict(cursor)
        q = 'SELECT * FROM view_clash where is_active = 0 ORDER BY end_at DESC LIMIT 4'
        cursor.execute(q)
        rows2 = sql_rows_dict(cursor)
        return rows1 + rows2

    cursor.execute(q)
    rows = sql_rows_dict(cursor)
    
    if cid is not None and len(rows) == 0:
        return None
    if cid is not None:
        return rows.pop()
    
    return rows

def format_time(ends_at):
    if ends_at is not None:
        return ends_at.astimezone().isoformat()
    else:
        return ends_at



def format_body_player(data):
    if isinstance(data, list):
        objs = [format_body_player(obj) for obj in data]
        return sorted(objs, key=lambda obj: obj['name'])
    
    if data['num_complete'] == 0:
        efficiency = 0
    else:
        efficiency = f'{100.0 * int(data["num_won"]) / int(data["num_complete"]):.2f}'
    
    return {
        'pid':              int(data['player_id']),
        'name':             format_body_name(data['fname'], data['lname']),
        'handed':           format_body_handed(data['handed']),
        'is_active':        bool(data['is_active']),
        'num_join':         int(data['num_join']),
        'num_won':          int(data['num_won']),
        'num_dq':           int(data['num_dq']),
        'balance_usd':      format_body_currency(data['balance_usd']),
        'total_points':     int(data['total_points']),
        'rank':             int(data['player_rank']),
        'spec_count':       0,
        'total_prize_usd':  format_body_currency(data['total_prize_usd']),
        'efficiency':       efficiency,
    }

def format_body_clash(data):
    if isinstance(data, list):
        objs = [format_body_clash(obj) for obj in data]
        return objs
    
    return {
        'cid':              int(data['clash_id']),
        'p1_id':            int(data['player1_id']),
        'p1_name':          format_body_name(data['p1_fname'], data['p1_lname']),
        'p1_points':        int(data['p1_points']),
        'p2_id':            int(data['player2_id']),
        'p2_name':          format_body_name(data['p2_fname'], data['p2_lname']),
        'p2_points':        int(data['p2_points']),
        'winner_pid':       data['winner_pid'] or None,
        'is_active':        bool(data['is_active']),
        'prize_usd':        str(data['prize_usd']),
        'age':              int(data['age_sec']),
        'ends_at':          format_time(data['end_at']),
        'attendance':       0   
    }

def format_body_playerBalance(data): ##not used
    if isinstance(data, list):
        objs = [format_body_clash(obj) for obj in data]
        return objs
    
    return {
        "old_balance_usd":    str(data['old_balance_usd']),
        "new_balance_usd":    str(data['new_balance_usd'])
    }

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        r = self._do_request()
        self.respond(*r)

    def do_POST(self):
        r = self._do_request()
        self.respond(*r)

    def respond(self, http_code, body, extraHeaders = {}):
        self.send_response(http_code)
        print("response code: ", http_code, ' body: ', body)
        for key, val in extraHeaders.items():
            print(key, val)
            self.send_header(key, val)
        
        self.end_headers()

        if not isinstance(body, str):
            body = json.dumps(body)
        
        self.wfile.write(body.encode('ascii'))
    
    def _do_request(self):
        path, _, query_string = self.path.partition("?")
        query = parse_qs(query_string, keep_blank_values=True)
        query = self.sanitize_query(query)

        HANDED_MAP = {}
        HANDED_MAP['L'] = 'left'
        HANDED_MAP['R'] = 'right'
        HANDED_MAP['AMBI'] = 'ambi'

        try:
            # GET REQUEST
            if self.command.upper() == 'GET':
                if path == '/ping':
                    return req_ping()
                elif path == '/player':
                    return req_player_list()
                elif m := re.match(r'^/player/(\d+)$', path):
                    pid = m[1]
                    return req_player_get(pid)
                elif path == '/clash':
                    return req_clash_list()
                elif m := re.match(r'^/clash/(\d+)$', path):
                    cid = m[1]
                    return req_clash_get(cid)
            
            # POST REQUEST
            elif self.command.upper() == 'POST':
                if path == '/admin/pre':
                    return req_admin_pre()

                elif path == '/player':
                    fname = query.get('fname', None)
                    lname = query.get('lname', None)
                    handed = query.get('handed', None)
                    balance_usd = query.get('initial_balance_usd', None)

                    errs = {
                        'fname':               is_valid_fname(fname),
                        'handed':              is_valid_handed(handed),
                        'initial_balance_usd': is_valid_currency(balance_usd),
                        'lname':               is_valid_lname(lname)
                    }

                    #gather failed input fields
                    errs = list(filter(lambda key: not errs[key], errs.keys()))

                    if len(errs) > 0:
                        return 422, f'Error: {",".join(errs)}'
                    
                    #Handed, req -> db
                    h_keys = list(HANDED_MAP.keys())
                    h_vals = list(HANDED_MAP.values())
                    handed = h_keys[h_vals.index(handed)]

                    return req_player_create(fname, lname, handed, balance_usd)
                
                elif m := re.match(r'^/player/(\d+)$', path):
                    pid = m[1]
                    print("pid: ",pid)
                    print("-----------")
                    lname = query.get('lname', None)
                    is_active = query.get('active', None)
                    is_active = to_boolean(is_active)

                    return req_player_update(pid, lname, is_active)
                
                elif m := re.match(r'^/clash/(\d+)end$', path):
                    cid = m[1]

                    return req_clash_end(cid)
                
                elif m := re.match(r'^/clash/(\d+)/disqualify/(\d+)$', path):
                    cid = m[1]
                    pid = m[2]

                    return req_clash_dq(cid, pid)
                
                elif m := re.match(r'^/clash/(\d+)/award/(\d+)$', path):
                    cid = m[1]
                    pid = m[2]
                    points = query.get('points', None)
                    print(points)
                    print(is_valid_points(points))
                    if not is_valid_points(points):
                        return 400, ''
                    print("here")
                    return req_clash_award(cid, pid, points)

                elif m := re.match(r'^/deposit/player/(\d+)$', path):
                    pid = m[1]
                    amount_usd = query.get('amount_usd', None)

                    if not is_valid_currency(amount_usd):
                        return 400, ''
                    
                    return req_player_deposit(pid, amount_usd)

                elif path == '/clash':
                    pid1 = query.get('pid1', None)
                    pid2 = query.get('pid2', None)
                    entry_fee_usd = query.get('entry_fee_usd', None)
                    prize_usd = query.get('prize_usd', None)

                    errs = {
                        'pid1':          is_valid_id(pid1),
                        'pid2':          is_valid_id(pid2),
                        'entry_fee_usd': is_valid_currency(entry_fee_usd),
                        'prize_usd':     is_valid_currency(prize_usd)
                    }

                    # gather failed input fields
                    errs = list(filter(lambda key: not errs[key], errs.keys()))

                    if len(errs) > 0:
                        return 400, f'Error: {",".join(errs)}'

                    return req_clash_create(pid1, pid2, entry_fee_usd, prize_usd)
                
                elif m := re.match(r'^/clash/(\d+)/end$', path):
                    cid = m[1]

                    return req_clash_end(cid)
                
                    
        except Exception as err:
            print('ERR1', self.command, path, err)
            print(err)
            return req_404()
        
        return req_404()

    def sanitize_query(self, query = {}):
        for key in query.keys():
            if len(query[key]) == 1:
                query[key] = query[key].pop()

        return query


EXTEND_SCHEMA_SQL = [
  '''
  CREATE OR REPLACE VIEW 
  view_clash_player AS (
    SELECT
      c.clash_id,
      p.player_id,
      IF(p.player_id=c.player1_id, c.player2_id, c.player1_id) AS other_id,
      COALESCE(SUM(cp.points), 0) AS points,
      COALESCE(COUNT(cp.is_dq = TRUE) > 0, FALSE) AS is_dq
    FROM
      clash AS c
    LEFT JOIN
      player AS p
      ON 
        p.player_id=c.player1_id OR
        p.player_id=c.player2_id
    LEFT JOIN
      clash_point AS cp
      ON 
        cp.clash_id = c.clash_id AND
        cp.player_id = p.player_id
    GROUP BY
      c.clash_id,
      p.player_id
  )
  ''',
  '''
  CREATE OR REPLACE VIEW
  view_clash AS (
    SELECT
      c.clash_id,
      c.player1_id,
      p1.fname AS p1_fname,
      p1.lname AS p1_lname,
      COALESCE(cp1.points, 0) AS p1_points,
      c.player2_id,
      p2.fname AS p2_fname,
      p2.lname AS p2_lname,
      COALESCE(cp2.points, 0) AS p2_points,
      IF(c.end_at IS NULL, NULL, 
        CASE
          WHEN cp1.is_dq THEN c.player2_id
          WHEN cp2.is_dq THEN c.player1_id
          WHEN COALESCE(cp1.points, 0) > COALESCE(cp2.points, 0) THEN c.player1_id
          WHEN COALESCE(cp2.points, 0) > COALESCE(cp1.points, 0) THEN c.player2_id
        END
      ) AS winner_pid,
      CASE
        WHEN cp1.is_dq THEN c.player2_id
        WHEN cp2.is_dq THEN c.player1_id
        WHEN COALESCE(cp1.points, 0) > COALESCE(cp2.points, 0) THEN c.player1_id
        WHEN COALESCE(cp2.points, 0) > COALESCE(cp1.points, 0) THEN c.player2_id
      END AS leader_pid,
      (c.end_at IS NULL) AS is_active,
      c.prize_usd,
      TIME_TO_SEC(TIMEDIFF(NOW(),  c.create_at)) AS age_sec,
      c.end_at,
      c.attendance
    FROM
      clash AS c
    LEFT JOIN
      player AS p1
      ON p1.player_id = c.player1_id
    LEFT JOIN
      player AS p2
      ON p2.player_id = c.player2_id
    LEFT JOIN
      view_clash_player AS cp1
      ON
        cp1.clash_id = c.clash_id AND
        cp1.player_id = c.player1_id
    LEFT JOIN
      view_clash_player AS cp2
      ON
        cp2.clash_id = c.clash_id AND
        cp2.player_id = c.player2_id
  )
  ''',
  '''
  CREATE OR REPLACE VIEW
  view_clash_by_player AS (
    SELECT
      cp1.player_id,
      COUNT(cp1.player_id = cp2.player_id) AS num_join,
      SUM(cp1.is_dq) AS num_dq,
      SUM(cp1.points) AS total_point
    FROM
      view_clash_player AS cp1
    LEFT JOIN
      view_clash_player AS cp2
    ON
      cp2.clash_id = cp1.clash_id AND
      cp2.player_id = cp1.other_id
    GROUP BY (cp1.player_id)
  )
  ''',
  '''
  CREATE OR REPLACE VIEW
  view_player_pre AS (
    SELECT
        p.player_id,
        COALESCE(COUNT(vc.end_at), 0) AS num_complete,
        COALESCE(SUM(p.player_id = vc.winner_pid), 0) AS num_won,
        IF(vc.end_at IS NOT NULL, NULL, vc.clash_id) AS in_active_clash,
        SUM(IF(p.player_id = vc.winner_pid, vc.prize_usd, 0)) AS total_prize_usd
    FROM
        player AS p
    LEFT JOIN
        view_clash AS vc
    ON
        p.player_id = vc.player1_id OR
        p.player_id = vc.player2_id
    GROUP BY
        p.player_id, vc.clash_id
  )
  ''',
  '''
  CREATE OR REPLACE VIEW
  view_player AS (
    SELECT
        p.player_id,
        p.fname,
        p.lname,
        p.handed,
        p.is_active,
        vpp.num_complete,
        COALESCE(vcbp.num_join, 0) AS num_join,
        COALESCE(SUM(vpp.num_won), 0) AS num_won,
        COALESCE(vcbp.num_dq, 0) AS num_dq,
        p.balance_usd,
        COALESCE(vcbp.total_point, 0) AS total_points,
        COALESCE(SUM(vpp.total_prize_usd), 0) AS total_prize_usd,
        RANK() OVER (ORDER BY SUM(vpp.num_won) DESC) AS player_rank,
        COALESCE(SUM(vpp.num_won) / IF(SUM(vpp.in_active_clash) IS NULL, vcbp.num_join, vcbp.num_join-1), 0) AS efficiency,
        SUM(vpp.in_active_clash) AS in_active_clash
    FROM
        player AS p
    LEFT JOIN
        view_clash_by_player AS vcbp
    ON
        p.player_id = vcbp.player_id
    LEFT JOIN
        view_player_pre AS vpp
    ON
        p.player_id = vpp.player_id
    GROUP BY
        p.player_id, vcbp.num_join, vpp.num_complete
  )
  '''
]

dbconfig = read_db_config()
db = MySQLConnection(**dbconfig)
cursor = db.cursor(buffered=True)

httpd = HTTPServer(('', port), MyHTTPRequestHandler)
httpd.serve_forever()

        

