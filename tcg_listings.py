import requests
import json
import pandas as pd
import time
from datetime import datetime
from google.cloud import bigquery


def query_api(index, size):
    url = "https://mp-search-api.tcgplayer.com/v1/search/request?q=&isList=false&mpfev=2487"

    payload = json.dumps(
        {
            "algorithm": "sales_synonym_v2",
            "from": index,
            "size": size,
            "filters": {
                "term": {
                    "productLineName": ["flesh-and-blood-tcg"],
                    "setName": [
                        "rosetta",
                        "part-the-mistveil",
                        "heavy-hitters",
                        "bright-lights",
                        "dusk-till-dawn",
                        "outsiders",
                        "dynasty",
                    ],
                },
                "range": {},
                "match": {},
            },
            "listingSearch": {
                "context": {"cart": {}},
                "filters": {
                    "term": {"sellerStatus": "Live", "channelId": 0},
                    "range": {"quantity": {"gte": 1}},
                    "exclude": {"channelExclusion": 0},
                },
            },
            "context": {"cart": {}, "shippingCountry": "US", "userProfile": {}},
            "settings": {"useFuzzySearch": True, "didYouMean": {}},
            "sort": {},
        }
    )
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        # "cookie": "optimizelyEndUserId=oeu1715188353747r0.7344262453542667; tracking-preferences={%22version%22:1%2C%22destinations%22:{%22Actions%20Amplitude%22:true%2C%22AdWords%22:true%2C%22Drip%22:true%2C%22Facebook%20Pixel%22:true%2C%22Google%20AdWords%20New%22:true%2C%22Google%20Enhanced%20Conversions%22:true%2C%22Google%20Tag%20Manager%22:true%2C%22Hotjar%22:true%2C%22Impact%20Partnership%20Cloud%22:true%2C%22Optimizely%22:true}%2C%22custom%22:{%22advertising%22:true%2C%22functional%22:true%2C%22marketingAndAnalytics%22:true}}; tcg-uuid=3bc6b0c0-800f-44d7-a804-c3e6978322e2; tcgpartner=PK=EDHREC&M=1; __ssid=118ecf0ae05c8302370f83363486ba7; ajs_anonymous_id=83e2140f-9697-4e90-9e53-691b67362b56; _fbp=fb.1.1715188355480.809641892; _gcl_au=1.1.1810813594.1715188355; _hjSessionUser_1176217=eyJpZCI6IjAyYTc2MDcxLWMzNzQtNTc1ZC04NjYxLTU1N2IwOTdmOGM5ZSIsImNyZWF0ZWQiOjE3MTUxODgzNTU0MzIsImV4aXN0aW5nIjp0cnVlfQ==; product-display-settings=sort=price+shipping&size=10; setting=CD=US&M=1; _gcl_gs=2.1.k1^$i1718327835; _gcl_aw=GCL.1718327837.Cj0KCQjwsaqzBhDdARIsAK2gqncApObVWghi14B2dD0aU47AGoQ6AUgrR4QW__f4uTkuaGGpbXPLmYEaAjl-EALw_wcB; _gid=GA1.2.1879779140.1719346482; _ga=GA1.1.1241471496.1715188356; _ga_N5CWV2Q5WR=GS1.2.1719346481.1.0.1719346481.60.0.0; TCG_VisitorKey=a817b729-cb9f-4236-a6af-84552b09bb42; analytics_session_id=1719347328215; SellerProximity=ZipCode=&MaxSellerDistance=1000&IsActive=false; _hjSession_1176217=eyJpZCI6ImUzOTI0YjFiLWM2M2MtNDU1NC05MDgxLTY3YjQzOTc5YTk3MSIsImMiOjE3MTkzNDczMjg2ODcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=; SearchSortSettings=M=1&ProductSortOption=BestMatch&ProductSortDesc=False&PriceSortOption=Shipping&ProductResultDisplay=grid; ASP.NET_SessionId=hgqfxdncm5gnckb4tso0vcyy; valid=set=true; TCG_Data=M=1&SearchGameNameID=magic; tcg_analytics_previousPageData=%7B%22title%22%3A%22TCGplayer%3A%20Shop%20Flesh%20and%20Blood%20TCG%20Cards%2C%20Packs%2C%20Booster%20Boxes%22%2C%22href%22%3A%22https%3A%2F%2Fshop.tcgplayer.com%2Fflesh-and-blood-tcg%3FnewSearch%3Dtrue%26_gl%3D1*1mv7nub*_gcl_aw*R0NMLjE3MTgzMjc4MzcuQ2owS0NRandzYXF6QmhEZEFSSXNBSzJncW5jQXBPYlZXZ2hpMTRCMmREMGFVNDdBR29RNkFVZ3JSNFFXX19mNHVUa3VhR0dwYlhQTG1ZRWFBamwtRUFMd193Y0I.*_gcl_au*MTgxMDgxMzU5NC4xNzE1MTg4MzU1*_ga*MTI0MTQ3MTQ5Ni4xNzE1MTg4MzU2*_ga_VS9BE2Z3GY*MTcxOTM0NjQ4MS45LjEuMTcxOTM0ODQxMS4xNy4wLjA.%22%7D; _drip_client_4160913=vid%253D874dd812c3a04b25ac54e89f513b28e7%2526pageViews%253D34%2526sessionPageCount%253D17%2526lastVisitedAt%253D1719349564785%2526weeklySessionCount%253D1%2526lastSessionAt%253D1719346481818; _ga_VS9BE2Z3GY=GS1.1.1719346481.9.1.1719349691.60.0.0; _ga_XTQ57721TQ=GS1.1.1719346481.9.1.1719349691.0.0.0; tcg-segment-session=1719347327723%257C1719349693091; analytics_session_id.last_access=1719349693209",
        # "origin": "https://www.tcgplayer.com",
        # "priority": "u=1, i",
        # "referer": "https://www.tcgplayer.com/",
        # "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Microsoft Edge";v="126"',
        # "sec-ch-ua-mobile": "?0",
        # "sec-ch-ua-platform": '"Windows"',
        # "sec-fetch-dest": "empty",
        # "sec-fetch-mode": "cors",
        # "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


def send_to_bigquery(df):
    TABLEID = "card_games.listings"
    bigquery_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    job = bigquery_client.load_table_from_dataframe(df, TABLEID, job_config=job_config)

    job.result()


def extract_data():
    results = []
    index = 0
    size = 50
    data = query_api(index, 1)
    total_results = data["results"][0]["totalResults"]
    iterations = int(total_results / size) + 1

    for i in range(iterations):
        increment = index + size

        remaining_results = total_results - index
        if increment <= total_results:
            size = size
        elif remaining_results > 0:
            size = total_results - index
        else:
            break

        print(
            f"iteration: {i}, index: {index}, size: {size}, increment:{increment}, remaining_results: {remaining_results}"
        )
        data = query_api(index, size)
        for card in data["results"][0]["results"]:
            card_dict = {
                "card_game": card.get("productLineUrlName"),
                "tcg_product_id": card.get("productId"),
                # "card_name": card.get("productName").replace('"', ""),
                # "set_name": card.get("setName"),
                "card_id": card.get("customAttributes").get("number"),
                # "release_date": card.get("customAttributes").get("releaseDate"),
                # "rarity": card.get("rarityName"),
                "market_price": card.get("marketPrice"),
                "min_price_with_shipping": card.get("lowestPriceWithShipping"),
                "min_price": card.get("lowestPrice"),
                "total_listings": card.get("totalListings"),
                "card_score": card.get("score"),
                # 'card_description': card['customAttributes']['description'],
                # 'card_detail': card['customAttributes']['detailNote'],
                # 'card_type': card['customAttributes']['cardType'][0],
                # 'card_sub_type': card['customAttributes']['cardSubType'][0],
                # 'card_type': card['customAttributes']['cardType'][0],
                # 'defense': card['customAttributes']['defenseValue'],
                # 'life': card['customAttributes']['life'],
                # 'power': card['customAttributes']['power'],
                # 'cost': card['customAttributes']['cost'],
            }
            results.append(card_dict)

        index = index + size
        time.sleep(2)
    return results


data = extract_data()
df = pd.DataFrame(data)
df["load_date"] = datetime.today()
df["load_datetime"] = datetime.today().strftime("%Y-%m-%d")
df = df[~df["card_id"].isnull()]
# print(df.head())
# df.to_csv("data.csv", index=False)
send_to_bigquery(df)
