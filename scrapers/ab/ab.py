# scrapers/supermarkets/ab.py

import requests
from bs4 import BeautifulSoup
import json
from time import sleep


HEADERS = {
    "accept": "*/*",  
    "accept-language": "en-GB,en;q=0.9,el-GR;q=0.8,el;q=0.7,en-US;q=0.6",
    "apollographql-client-name": "gr-ab-web-stores",
    "apollographql-client-version": "963cf53bdf82d4ea97406e2c4bd4e7843383f4e5",
    "content-type": "application/json",
    "Cookie": "rxVisitor=1765818639930BSGP1MGDOO1H1T9AQ8PEULDH9CTS0RF5; dtPC=-2158$18639929_377h1vJMJBDDRJAUTCRHPGGMJMAFGRACSGSKEA-0e0; dtSa=-; deviceSessionId=ce2730e0-8d6f-4540-8fae-55d225158978; dtCookie=v_4_srv_1_sn_FCE300DB6E58D6617B0502E2148FE493_perc_100000_ol_0_mul_1_app-3A440a591b5a5302d3_0; _abck=B3F4E861D2E8EE748A6EE82E0311FC45~-1~YAAQxPUWAtyLk/iaAQAA5iT+Ig+8VrmvPfbOVG5RQtZvbRSgwooO8rFMKnyHelk+yC9PpJu2qhCM5zx3HmQcxYHtd2NMTbtpgeh0FXW0d6LpiSC+gnVa8GhB9UA0bEja5FMGR3T/89mfEjl9WUhX3XAHnLKp+x19l0vnaD+RwyPhYUqCupDFPhdPhtQU83GlzihyOrYtQFpdOpqo4Dxpyl52TJVN5y+Rf4NrsMu7V+ykglwZt9J+s6ku/qFelnDfmvNlY5+/TysTaMnZKbz2uA7ixvOAQuPQpk2icsxq7TuZZX2IlF43mada3y+SLCiZTHbNiuKYC3hRwDHm6jAeS5r8882CEfW0brKNgHk9ub3X6zd3KBPGwSm1kc4vDwiXbywZSkbqBmkZ0tx7d6vKCrSACVwtjLbbaKQIRlmeqcTknjww6+gmbq+nWZgXJT7xiW2F5RJEaP+a7/KRQf3zKPZ+bYV1urQCllbUK724Q+U=~-1~-1~-1~-1~-1; groceryCookieLang=gr; liquidFeeThreshold=0; rxvt=1765820441022|1765818639930; VersionedCookieConsent=v%3A2%2Cessential%3A1%2Canalytics%3A1%2Csocial%3A1%2Cperso_cont%3A1%2Cperso_ads%3A1%2Cads_external%3A1; v_cust=0; at_check=true; s_pls=not%20logged; s_fid=07C952A5AE22DC96-3409650523CD293B; gpv_loginStatus=not%20logged; s_cc=true; _fbp=fb.1.1765818644902.441365100762978968; _gcl_au=1.1.1237928341.1765818645; AMCV_2A6E210654E74B040A4C98A7%40AdobeOrg=-1124106680%7CMCMID%7C58951090103684041349145893009807792081%7CMCAID%7CNONE%7CvVersion%7C5.2.0%7CMCIDTS%7C20438; kndctr_2A6E210654E74B040A4C98A7_AdobeOrg_identity=CiY1ODk1MTA5MDEwMzY4NDA0MTM0OTE0NTg5MzAwOTgwNzc5MjA4MVIRCJWk-5eyMxgCKgRJUkwxMAPwAZWk-5eyMw==; kndctr_2A6E210654E74B040A4C98A7_AdobeOrg_cluster=irl1; bm_mi=E8637E476BC0991AE9BEC836C361094D~YAAQDDMTAmwjpwmbAQAAFdn/Ih5nZxChZSzzAXzWgoJ0nZPNL8t280kTM6QI84stEMezkGVj5zGExa7ZbsrNd8y8U6vX9xJBDwX0Jhw4Lxn0e0Naq4DunTWU0FKPxVQFcnXOu6AiK3zLlQ8JjvyMWxP+qbj4rK8qeKzM7H4xLdI5jK2Zd98OfAl+tRKYN2LfS64j4m6u+cQ3NOm47fnF1myzOE/IEUU6+Ptaau2jXZQVr0DaKhafA/h4RvPTi5dkNfhTe6mmNRDuxsAmlvCRc0V/db4mLMtKh0v40Z3RzEIgeXEH7YSx2F64Ig==~1; ak_bmsc=77D95374720C9FB3633543DB29259CD6~000000000000000000000000000000~YAAQxPUWApW4k/iaAQAAx5ABIx5BVtnSHaLVzSoC5cLRkcaT3FwCSWGfZx/nleHtxzMTAk7pygRHBwckJoZ1H/vjkKBl+ItMQ4WEwwMbZi5FtNfCICiqW9Qln1rPcrkXzj4leiJa/a0C2E9fuhX4g+AKuVI0FFtEdRcYq0NZeCM09vUjtUosY58P/p5LLnaH831IiMnLHm4qBHPph+I3j8aoEFEdZvl6vK6djNGB4K2x1gOj3c25LNNv00shv5a5E4uoYJF0Upr5gDQFcE5988LUAucVLGO++YBPwRQ51A/pT7BRw+3QpGZRRFygeQYRkJlTP0YDSbjVwOdjW8Ru7fnMCfkg+RXFdoJuJmWTJek9wCni9mKZUfGt2YiIJ+D7XN57UzP7u4zVsp50+nLqSM5ayxzVop2gl458E7asZXVCnlaT6Nrn1aGZyCtSm9IraVi4a5/TWCUUY/XYa7U8qCQezFhG/PBJAn/8/XIHGFh40ONkWykR3b04mRUXhiI=; AWSALB=jALQxsmTg/pA3uGHiwgW0tQrT48awuCy5M/7jrNMexFB9YSssObuOjYafONIFdyBknOHu2VExd0PktNNPXP90cU3Cabv0om9BPc98xhr8AWgflNU5MhAGBI80Kbh; AWSALBCORS=jALQxsmTg/pA3uGHiwgW0tQrT48awuCy5M/7jrNMexFB9YSssObuOjYafONIFdyBknOHu2VExd0PktNNPXP90cU3Cabv0om9BPc98xhr8AWgflNU5MhAGBI80Kbh; s_ppn=market%3Acategory%3Acategory-listing; s_sq=%5B%5BB%5D%5D; bm_sv=FC4AA6D507EA3FFD3DBFB5283699642F~YAAQxPUWAknZlPiaAQAAbp4XIx6Cna03PSDFoZiz/h+2rI0NDnn3FJKRZHhDvdAVnOTqFRVmrjV5AFCkgUGRcAC67Cvoer3/REawa7B9tUphWHXjRcFEIV7f8v/Qh7id+M3YcmfiDtHLKqFa3xsxpB0oL+L3Az8Okd64qeX0l+NOsmAh11xRx/TnHHXoC8P9suZeekKmviVzHCUDkImYroQDjzv+824o4C51jZzqw/LqFknk5Ieamz9qTkvle7fO~1; bm_sz=89E8F7A9FC78ED5D7E05946A4EFE1A25~YAAQxPUWAkrZlPiaAQAAbp4XIx72c28bAlWSZkmX8jexaF0tBxP+HPa6LzEeRuM6vBS33pjz4HoveZiIJMOxQc+txvAk0I8yODenphONaujcRxJzNdoOPXp3b5cW+GThQZ9rg0JnORVy3xllK9G3US89uYE9LP4k62kgWI1YWcTfZAHFyexv8Kx1895mM/kvdFG/Irv8waRC5BvLNNEllgg8lgn2LXlc8Hr49uzOP3Wd6XucawmQxev0BDlhx34MiN1olvPHHJ230aLlJudBkiyi8TK3bfN2jbUQZOKWkMZKHnjiSpm+uxmePIn/l45ajmXKadPNTekkdmBD8qL+Jni1kTXNYgvn2kRf8iNKZ0SrmD3W5FDwgHPgJK3cOsAxIbkLDZz1mC0vBY5I+R/i1yC3pIUxN/CXRCFVnoBYtV5M8S9Tsq9hkkM6bjKHAVQFWWiro3d5VJE2dPqu/aqY/zrVV7+0ksGqH2CJNNxT9ZRteCyIuNBsD3lcXkLPWm5V3pF3t/T0141fvz+8siiBtjXRRFZxPhNIESU=~3684673~3425334; mbox=session#1c0014ede8ab4a47b224baaa9e74346a#1765822170|PC#1c0014ede8ab4a47b224baaa9e74346a.37_0#1829065110",
    "priority": "u=1, i",
    "referer": "https://www.ab.gr",
    "sec-ch-ua-mobile": "?1",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36",
}


def fetch_categories():
    url = "https://www.ab.gr/"
    html = requests.get(url, headers=HEADERS).text
    soup = BeautifulSoup(html, "html.parser")

    navbar = soup.find("ul", attrs={"data-testid":"category-carousel-list"})


    return [
        "https://www.ab.gr/" + a["href"]
        for a in navbar.find_all("a")
        if "/c/" in a.get("href", "")
    ]


def fetch_products(category_code, page):
    url = "https://www.ab.gr/api/v1/"

    params = {
        "operationName": "GetCategoryProductSearch",
        "variables": json.dumps({
            "lang": "gr",
            "category": category_code,
            "pageNumber": page,
            "pageSize": 50,
            "fields": "PRODUCT_TILE"
        }),
        "extensions": json.dumps({
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "afce78bc1a2f0fe85f8592403dd44fae5dd8dce455b6eeeb1fd6857cc61b00a2"
            }
        })
    }

    response = requests.get(url, headers=HEADERS, params=params)

    return response.json()


def scrape():

    products = []
    categories = fetch_categories()
    for category_url in categories:
        sleep(2)
        category_code = category_url.split("c/")[1]
        print(category_code)
        first_page = fetch_products(category_code, 0)

        
        pagination = first_page["data"]["categoryProductSearch"]["pagination"]
        total_pages = pagination["totalPages"]

        products.extend(
            first_page["data"]["categoryProductSearch"]["products"]
        )

        for page in range(1, total_pages):
            data = fetch_products(category_code, page)
            products.extend(
                data["data"]["categoryProductSearch"]["products"]
            )
        break


    return products
