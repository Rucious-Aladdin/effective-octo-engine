from urllib.request import *
import time
from bs4 import *

def cal_edit_distance(target, name):
    pass

def stockprice(company_code, period):

    pList = list()
    DN = period
    PN = DN // 20
    if DN > PN * 20 :
        PN += 1

    for i in range(1,PN+1) :
        url = 'http://finance.naver.com/item/frgn.nhn?code='+company_code+'&page=' + str(i)
        req = Request(url)
        req.add_header('User-Agent','Mozilla/5.0')
        wp = urlopen(req)
        soup = BeautifulSoup(wp, 'html.parser')
        trList = soup.find_all('tr',{'onmouseover':'mouseOver(this)'})

        for n in trList :
            tdList = n.find_all('td')
            price = tdList[1].get_text()
            price = price.replace(',','')
            pList.append(int(price))
            
    pList.reverse()
    time.sleep(0.1)
    return pList