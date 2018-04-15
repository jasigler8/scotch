import requests
import bs4
import psycopg2
import datetime

REGIONS = ['Campbeltown','Highlands','Islands','Islay','Lowlands','Speyside']
# REGIONS = ['Islay']
GATEWAY = 'http://www.astorwines.com/spiritssearchresult.aspx?search=Advanced&searchtype=Contains&cat=0&country=Scotland&region='
BOTTLES_PER_PAGE = 12

HOST = 'localhost'
DB = 'astor'
DB_UID = 'postgres'
DB_PWD = 'fratcastle'
TODAY = str(datetime.datetime.today().strftime('%Y%m%d'))
TABLE = 'astor_' + TODAY

def http_get(url):
    r = requests.get(url)
    try:
        r.raise_for_status()
    except Exception as exc:
        print('There was a problem accessing ' + url)
    return r

def scrape(r):
    webpage = bs4.BeautifulSoup(r.text, "html.parser")
    return webpage

def count_total(webpage):
    total_bottles = webpage.select('div.col-xs-12.col-md-5 div div div')[0].text.split()[-1].replace(')', '')
    return total_bottles

def count_bottle_page(webpage):
    bottle_page_count = len(webpage.select('div.item-teaser'))
    return bottle_page_count

def bottle_data(webpage,bottle_page_count,bottle_name,bottle_size,bottle_num,bottle_price,bottle_region,REGIONS):
    for num in range(bottle_page_count):
        bottle_name.append(webpage.select('#middleContent_pageContent_WUCSearchResults1_datResults_hyplItemName_' + str(num))[0].text)
        try:
            bottle_size.append(webpage.select('div.item-meta.supporting-text > span.small')[num].text.split('|')[1])
        except IndexError as error:
            bottle_size.append('')
        bottle_num.append(webpage.select('span.itemNumber.text-muted.small')[num].text.split('#')[1])
        try:
            bottle_price.append(webpage.select('span.price-value.price-bottle.display-2')[num].text.replace('$',''))
        except IndexError as error:
            bottle_price.append(0)
        bottle_region.append(REGIONS[i])

def connect_db(host,db,id,pswd):
    conn = psycopg2.connect(host=host, database=db, user=id, password=pswd)
    cur = conn.cursor()
    return conn,cur

def drop_create_table(cur,table):
    drop_table = 'Drop Table If Exists ' + table + ';'
    cur.execute(drop_table,)
    create_table = 'Create table ' + table + "(name varchar(200),size varchar(50),item_num varchar(50),price float8,region varchar(50),date varchar(8))"
    cur.execute(create_table,)
    return True

def add_row(cur,table,today,bottle_name,bottle_size,bottle_num,bottle_price,bottle_region):
    add_row = "INSERT INTO " + table + " (name,size,item_num,price,region,date) values('{}','{}','{}',{},'{}',{});".format(bottle_name.replace("'",""), bottle_size, bottle_num, bottle_price, bottle_region, today)
    #data = (bottle_name, bottle_size, bottle_num, {}, bottle_region, today).format(bottle_price)
    cur.execute(add_row,)
    return True

def close_connection(cur,conn):
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':

    bottle_name = []
    bottle_size = []
    bottle_num = []
    bottle_price = []
    bottle_region = []

    for i in range(len(REGIONS)):
        current_url = GATEWAY + REGIONS[i]
        r = http_get(current_url)
        webpage = scrape(r)
        total_bottles = count_total(webpage)
        bottle_page_count = count_bottle_page(webpage)
        num_pages = int(int(total_bottles) / int(BOTTLES_PER_PAGE))+1

        bottle_data(webpage,bottle_page_count,bottle_name,bottle_size,bottle_num,bottle_price,bottle_region,REGIONS)

        if num_pages > 1:
            for x in range(2,num_pages+1):
                new_url = current_url + '&Page=' + str(x)
                r = http_get(new_url)
                webpage = scrape(r)
                bottle_page_count = count_bottle_page(webpage)
                bottle_data(webpage,bottle_page_count,bottle_name,bottle_size,bottle_num,bottle_price,bottle_region,REGIONS)

conn,cur = connect_db(HOST, DB, DB_UID, DB_PWD)

drop_create_table(cur, TABLE)

for y in range(len(bottle_name)):
    add_row(cur,TABLE,TODAY,bottle_name[y],bottle_size[y],bottle_num[y],bottle_price[y],bottle_region[y])

close_connection(cur, conn)

print('Script Complete')


