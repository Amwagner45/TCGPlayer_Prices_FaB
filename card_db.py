import pandas as pd
import json
from google.cloud import bigquery
import requests


def get_all_cards():
    r = requests.get(
        "https://raw.githubusercontent.com/the-fab-cube/flesh-and-blood-cards/develop/json/english/card.json"
    )
    return r.json()


def send_to_bigquery(df):
    TABLEID = "card_games.cards"
    bigquery_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = bigquery_client.load_table_from_dataframe(df, TABLEID, job_config=job_config)

    job.result()


cards = []

# with open("card.json", "r", encoding="UTF-8") as f:
#     data = json.load(f)

data = get_all_cards()

for card in data:
    card_data = {
        "set_code": card.get("printings")[0].get("set_id"),
        "card_id": card.get("printings").get("id"),
        "tcg_product_id": card.get("printings")[0].get("tcgplayer_product_id"),
        "card_name": card.get("name"),
        "rarity": card.get("printings")[0].get("rarity"),
        "pitch": card.get("pitch"),
        "cost": card.get("cost"),
        "attack": card.get("power"),
        "defense": card.get("defense"),
        "health": card.get("health"),
        "intelligence": card.get("intelligence"),
        # "types": ", ".join(card.get("types")),
        "types": card.get("types"),
        # "keywords": ", ".join(card.get("card_keywords")),
        "keywords": card.get("card_keywords"),
        "text": str(card.get("functional_text_plain")).replace(",", ""),
        "type": card.get("type_text"),
        "printings": card.get("printings"),
    }
    cards.append(card_data)

df = pd.DataFrame(cards)
# df["tcg_product_id"] = df["tcg_product_id"].astype(str)
# df.to_csv("card_data.csv", index=False, sep=",")
send_to_bigquery(df)
