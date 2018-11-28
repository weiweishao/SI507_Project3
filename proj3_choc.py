# Wei Shao
# Section 003
# I finish this project individually

import sqlite3
import csv
import json


# proj3_choc.py
# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def create_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS 'Bars';
        '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
        '''
    cur.execute(statement)
    conn.commit()

    # create table: Bars
    statement = '''
        CREATE TABLE 'Bars' (
            'id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL NOT NULL,
            'CompanyLocation' TEXT NOT NULL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL NOT NULL,
            'BeanType' TEXT NOT NULL,
            'BroadBeanOrigin' TEXT NOT NULL,
            'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE 'Countries' (
            'id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL,
            'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()

    

def populate_db():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    # from BARSCSV write raw data into Bars
    # Table Bars not complete, 2 foreign ids to be filled
    with open(BARSCSV) as b_data:
        b_csv = csv.reader(b_data)
        line_count =0
        for row in b_csv:
            if line_count == 0:
                line_count += 1
            else:
                cocoa = ( float(row[4].rstrip('%')) )/100
                insertion = (row[0], row[1], row[2], row[3], cocoa ,row[5],row[6],row[7],row[8])
                line_count += 1
                statement = '''
                    INSERT INTO 'Bars' (Company, SpecificBeanBarName, REF, ReviewDate,
                    CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin) 
                    VALUES (?,?,?,?,?,?,?,?,?)
                    '''
                cur.execute(statement, insertion)
                conn.commit()

    # from COUNTRIESJSON write data into Countries
    with open(COUNTRIESJSON) as c_data:
        c_json = json.load(c_data) 
        for row in c_json:
            name = row['name']
            alpha2 = row['alpha2Code']
            alpha3 = row['alpha3Code']
            region = row['region']
            subregion = row['subregion']
            pop = row['population']
            area = row['area']
            insertion = (alpha2, alpha3, name, region, subregion, pop, area)
            statement = '''
                    INSERT INTO 'Countries' (Alpha2, Alpha3, EnglishName, Region,
                    Subregion, Population, Area)
                    VALUES (?,?,?,?,?,?,?)
                    '''
            cur.execute(statement, insertion)
            conn.commit()
    conn.close()

    
def insert_location_id():
    # write LociationId into Bars
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        SElect Bars.id, Countries.EnglishName, Countries.id
        From Bars JOIN Countries
        On Bars.CompanyLocation = Countries.EnglishName
        '''
    cur.execute(statement)
    link = []
    for c in cur:
        link.append(c)

    for row in link:
        insertion = (row[2], row[0])
        statement = '''
                UPDATE Bars
                SET CompanyLocationId = ?
                WHERE Id = ?
                '''
        cur.execute(statement, insertion)
        conn.commit()
    conn.close()



def insert_bean_origin_id():
    # write BeanOriginId into Bars
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    statement = '''
        Select Bars.id, Countries.EnglishName, Countries.id
        From Bars JOIN Countries
        On Bars.BroadBeanOrigin = Countries.EnglishName
        '''
    cur.execute(statement)
    link = []
    for c in cur:
        link.append(c)

    for row in link:
        insertion = (row[2], row[0])
        statement = '''
                UPDATE Bars
                SET BroadBeanOriginId = ?
                WHERE Id = ?
                '''
        cur.execute(statement, insertion)
        conn.commit()
    conn.close()



create_db()
populate_db()
insert_location_id()
insert_bean_origin_id()

# ========================= End of Part 1 =====================
# Part 2: Implement logic to process user commands


# check if the command is valid
# parameter: command input by user
# return: a dictionary of formated command
def check_bad_command(command):
    check_lst ={}
    check_lst['command']= 'bad'
    check_lst['sort_by'] = 'ratings'
    check_lst['match'] = ['top','10']
    
    command_lst = command.split()
    command = command_lst[0]

    raw = []
    for i in command_lst:
        item = i.split(sep='=')
        raw.append(item)

    # format the command to be a list of values (give default values if not specified)
    # check for 'bars'
    if command == 'bars':
        for j in raw:
            if len(j)==1 and j == ['bars']:
                check_lst['command']='bars'
            elif len(j)==1 and j != ['bars']:
                if j == ['ratings'] or j == ['cocoa']:
                    check_lst['sort_by'] = j[0]
                else:
                    check_lst['command']='bad'
            if len(j)==2:
                if j[0] in ['sellcountry','sourcecountry','sellregion','sourceregion']:
                    check_lst['area'] = j
                elif j[0] in ['top','bottom']:
                    check_lst['match'] = j
                else:
                    check_lst['command']= 'bad'

    # check for 'companies'
    if command == 'companies':
        for j in raw:
            if len(j)==1 and j==['companies']:
                check_lst['command']='companies'
            elif len(j)==1 and j != ['companies']:
                if j in [['ratings'],['cocoa'],['bars_sold']]:
                    check_lst['sort_by'] = j[0]
                else:
                    check_lst['command']='bad'

            if len(j)==2:
                if j[0] in ['country','region']:
                    check_lst['area'] = j
                elif j[0] in ['top','bottom']:
                    check_lst['match'] = j
                else:
                    check_lst['command']= 'bad'
            

    # check for 'countries'
    if command == 'countries':
        check_lst['select_by']='sellers'
        for j in raw:
            if len(j)==1 and j==['countries']:
                check_lst['command']='countries'
            elif len(j)==1 and j != ['countries']:
                if j in [['ratings'],['cocoa'],['bars_sold']]:
                    check_lst['sort_by'] = j[0]
                elif j in [['sellers'],['sources']]:
                    check_lst['select_by']=j[0]
                else:
                    check_lst['command']='bad'
            if len(j)==2:
                if j[0] == 'region':
                    check_lst['area'] = j
                elif j[0] in ['top','bottom']:
                    check_lst['match'] = j
                else:
                    check_lst['command']= 'bad'
                    
    # check for 'regions'
    if command == 'regions':
        check_lst['select_by']='sellers'
        for j in raw:
            if len(j)==1 and j==['regions']:
                check_lst['command']='regions'
            elif len(j)==1 and j != ['regions']:
                if j in [['ratings'],['cocoa'],['bars_sold']]:
                    check_lst['sort_by'] = j[0]
                elif j in [['sellers'],['sources']]:
                    check_lst['select_by']=j[0]
                else:
                    check_lst['command']='bad'
            if len(j)==2:
                if j[0] in ['top','bottom']:
                    check_lst['match'] = j
                else:
                    check_lst['command']= 'bad'
    
    return check_lst


# list chocolate bars according to specified parameters
# paremeter: a pre-formated dictionary by check_bad_command()
def command_bars(dic):
    sort = ''
    if dic['sort_by'] == 'ratings':
        sort = 'Rating'
    if dic['sort_by'] == 'cocoa':
        sort = 'CocoaPercent'
        
    match = dic['match']
    limit = match[1]
    order = ' '
    if match[0]=='top':
        order = " DESC"
        
    if 'area' in dic.keys():
        area = dic['area']
    else:
        area = []

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    results = []
    
    if area == []:
        sql = ''' SELECT SpecificBeanBarName, Company, CompanyLocation,
                Rating, CocoaPercent, BroadBeanOrigin
                FROM Bars ORDER BY ''' + sort + order + ''' limit ?'''
        insertion = (limit,)
        cur.execute(sql,insertion)
        for i in cur:
            results.append(i)
  
    elif area != []:
        sql = '''SELECT a.SpecificBeanBarName, a.Company, a.CompanyLocation, b.Alpha2, b.Region,
            a.Rating, a.CocoaPercent, a.BroadBeanOrigin,c.Alpha2, c.Region
            FROM Bars as a JOIN Countries as b
            ON a.CompanyLocationId = b.Id
            JOIN Countries as c ON a.BroadBeanOriginId = c.Id '''
        
        if area[0]=='sellcountry':
            con = 'b.Alpha2'
        elif area[0]=='sourcecountry':
            con = 'c.Alpha2'
        elif area[0]=='sellregion':
            con = 'b.Region'
        elif area[0]=='sourceregion':
            con = 'c.Region'

        sql += ' Where ' + con + ' =? Order by a.' + sort + order + ' limit ?'
        insertion = (area[1], limit)    
        cur.execute(sql,insertion)
        for i in cur:
            row = (i[0],i[1],i[2],i[5],i[6],i[7])
            results.append(row)

    conn.close()
    return results

# list companies according to specified parameters
# paremeter: a pre-formated dictionary by check_bad_command()
def command_companies(dic):
    sort = ''
    loc = 0
    if dic['sort_by'] == 'ratings':
        sort = 'agg_rating'
        loc = 4
    elif dic['sort_by'] == 'cocoa':
        sort = 'agg_cocoa'
        loc = 5
    elif dic['sort_by'] == 'bars_sold':
        sort = 'SumSold'
        loc = 6
    match = dic['match']
    limit = match[1]
    order = ' '
    if match[0]=='top':
        order = " DESC"
        
    if 'area' in dic.keys():
        area = dic['area']
    else:
        area = []

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    results = []
    sql1 = ''' SELECT a.Company, a.CompanyLocation, b.Alpha2, b.Region,
            AVG(a.Rating) as agg_rating, AVG(a.CocoaPercent) as agg_cocoa,
            COUNT(DISTINCT a.SpecificBeanBarName) as SumSold
            FROM Bars as a JOIN Countries as b ON a.CompanyLocationId = b.Id '''
    sql2 = ' GROUP BY a.Company HAVING COUNT(a.SpecificBeanBarName) > 4 ORDER BY '
    if area == []:
        sql = sql1 +sql2 + sort + order + ' limit ?'
        insertion = (limit,)
    
    elif area != []:
        if area[0]=='country':
            con = 'b.Alpha2'
        elif area[0]=='region':
            con = 'b.Region'

        sql = sql1 + ' where ' + con + ' =? '+ sql2 + sort + order + ' limit ?'
        insertion = (area[1], limit)
      
    cur.execute(sql,insertion)
    for i in cur:
        if loc != 6:
            agg = float('{0:.2g}'.format(i[loc]))
        else:
            agg = i[loc]
        row = (i[0],i[1], agg)
        results.append(row)

    conn.close()
    return results

# list countries according to specified parameters
# paremeter: a pre-formated dictionary by check_bad_command()
def command_countries(dic):
    sort = ''
    loc = 0
    if dic['sort_by'] == 'ratings':
        sort = 'agg_rating'
        loc = 2
    elif dic['sort_by'] == 'cocoa':
        sort = 'agg_cocoa'
        loc = 3
    elif dic['sort_by'] == 'bars_sold':
        sort = 'SumSold'
        loc = 4
        
    match = dic['match']
    limit = match[1]
    order = ' '
    if match[0]=='top':
        order = " DESC"
        
    if 'area' in dic.keys():
        area = dic['area']
    else:
        area = []

    select = ''
    if dic['select_by']=='sellers':
        select = 'a.CompanyLocationId'
    elif dic['select_by']=='sources':
        select = 'a.BroadBeanOriginId'

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    results = []
    
    sql1 = ''' SELECT b.EnglishName, b.Region,
            AVG(a.Rating) as agg_rating, AVG(a.CocoaPercent) as agg_cocoa,
            COUNT(a.SpecificBeanBarName) as SumSold
            FROM Bars as a JOIN Countries as b'''
    sql2 = ' ON ' + select + ' = b.Id '
    sql3 = ' GROUP BY b.Id HAVING COUNT(a.SpecificBeanBarName) > 4 ORDER BY '
    sql4 = ' where b.region = ?'
    
    if area == []:
        sql = sql1 +sql2 + sql3 + sort + order + ' limit ?'
        insertion = (limit,)
    
    elif area != []:
        sql = sql1 + sql2 + sql4 + sql3 + sort + order + ' limit ?'
        insertion = (area[1], limit)
      
    cur.execute(sql,insertion)
    for i in cur:
        if loc != 4:
            agg = float('{0:.2g}'.format(i[loc]))
        else:
            agg = i[loc]
        row = (i[0],i[1], agg)
        results.append(row)

    conn.close()
    return results


# list regions according to specified parameters
# paremeter: a pre-formated dictionary by check_bad_command()
def command_regions(dic):
    sort = ''
    loc = 0
    if dic['sort_by'] == 'ratings':
        sort = 'agg_rating'
        loc = 1
    elif dic['sort_by'] == 'cocoa':
        sort = 'agg_cocoa'
        loc = 2
    elif dic['sort_by'] == 'bars_sold':
        sort = 'SumSold'
        loc = 3
        
    match = dic['match']
    limit = match[1]
    order = ' '
    if match[0]=='top':
        order = " DESC"

    select = ''
    if dic['select_by']=='sellers':
        select = 'a.CompanyLocationId'
    elif dic['select_by']=='sources':
        select = 'a.BroadBeanOriginId'

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    results = []
    
    sql1 = ''' SELECT b.Region, AVG(a.Rating) as agg_rating,
            AVG(a.CocoaPercent) as agg_cocoa, COUNT(a.SpecificBeanBarName) as SumSold
            FROM Bars as a JOIN Countries as b'''
    sql2 = ' ON ' + select + ' = b.Id '
    sql3 = ' GROUP BY b.region HAVING COUNT(a.SpecificBeanBarName) > 4 '
    sql4 = ' ORDER BY ' + sort + order + ' limit ?'
    
    insertion = (limit,)
    sql = sql1 + sql2 + sql3 + sql4
    
    cur.execute(sql,insertion)
    for i in cur:
        if loc != 3:
            agg = float('{0:.2g}'.format(i[loc]))
        else:
            agg = i[loc]
        row = (i[0], agg)
        results.append(row)

    conn.close()
    return results


# parameter: command is a string of command
# return: a list of tuples that match the command
def process_command(command): 
    dic = check_bad_command(command)
    result = []

    if dic['command'] == 'bars':
        result = command_bars(dic)
    elif dic['command'] == 'companies':
        result = command_companies(dic)
    elif dic['command'] == 'countries':
        result = command_countries(dic)
    elif dic['command'] == 'regions':
        result = command_regions(dic)  
    return result


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# ========================= End of Part 2 ================================
# Part 3: Implement interactive prompt. 


# parameter: the results from process_command(command), a list of tuples
# return: print nicely
def nice_format(string,num):
    if len(string) <= num:
        r1 = '{:'+str(num)+'}'
        r2 = r1.format(string)
    else:
        r1 = string[:(num-3)]
        r2 = r1 + '...'
    return r2
    


def print_nicely(lst, num):
    if num == 2:
        for i in lst:
            j1=nice_format(i[0],20)
            j2='{:5}'.format(i[1])
            row=j1+j2
            print(row)
    elif num == 3:
        for i in lst:
            j1=nice_format(i[0],20)
            j2=nice_format(i[1],20)
            j3='{:5}'.format(i[2])
            row=j1+j2+j3
            print(row)
    elif num == 6:
        for i in lst:
            j1=nice_format(i[0],15)
            j2=nice_format(i[1],15)
            j3=nice_format(i[2],15)
            j4='{:5}'.format(str(i[3]))
            j5='{:5}'.format(str(i[4]))
            j6=nice_format(str(i[5]),15)
            row=j1+j2+j3+j4+j5+j6
            print(row)
        

# main function
def interactive_prompt():
    help_text = load_help_text()
    response = input("Enter a command: ")
    
    while response != 'exit':
        if response == '':
            response = input('\nEnter a command: ')
        if response == 'help':
            print(help_text)
            response = input('\nEnter a command: ')
        else:
            raw_input = check_bad_command(response)
            if raw_input['command'] == 'bad':
                print('Command not recognized: ' + response)
                response = input('\nEnter a command: ')
            else:
                raw_output = process_command(response)
                # change the format of cocoapercent
                size = len(raw_output[0])
                output = []
                if size == 6:
                    for i in raw_output:
                        cocoa = str(int(i[4]*100)) + '%'
                        row = (i[0],i[1],i[2],i[3],cocoa,i[5])
                        output.append(row)
                elif size == 2 and raw_input['sort_by']=='cocoa':
                    for i in raw_output:
                        cocoa = str(int(i[1]*100)) + '%'
                        row = (i[0], cocoa)
                        output.append(row)
                elif size == 3 and raw_input['sort_by']=='cocoa':
                    for i in raw_output:
                        cocoa = str(int(i[2]*100)) + '%'
                        row = (i[0],i[1],cocoa)
                        output.append(row)
                else:
                    output = raw_output

                print_nicely(output, size)
                response = input('\nEnter a command: ')

    if response == 'exit':
        print('bye')
        pass

    



# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
