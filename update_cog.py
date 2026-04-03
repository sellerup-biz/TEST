"""
PROCARE — обновление себестоимости (COG) из Excel-файла
Читает priceVAT.xls (столбцы: SKU или EAN, цена)
Сопоставляет с products.json по EAN/offer_id
Обновляет поле cog для каждого магазина

Запуск: python update_cog.py
Или через upload_cog.yml (GitHub Actions)
"""
import json, os, sys

try:
    import pandas as pd
except ImportError:
    print("❌ pandas не установлен. pip install pandas openpyxl xlrd")
    sys.exit(1)

# ── Загрузка конфига ─────────────────────────────────────────
config_path = os.path.join(os.path.dirname(__file__) or ".", "config.json")
with open(config_path, encoding="utf-8") as f:
    CONFIG = json.load(f)

SHOP_NAMES = [s["name"] for s in CONFIG["shops"]]

# ── Поиск файла с ценами ─────────────────────────────────────
PRICE_FILES = ["priceVAT.xls", "priceVAT.xlsx", "prices.xls", "prices.xlsx", "prices.csv"]
price_file = None
for pf in PRICE_FILES:
    if os.path.exists(pf):
        price_file = pf
        break

if not price_file:
    print(f"❌ Файл с ценами не найден. Ожидается один из: {', '.join(PRICE_FILES)}")
    sys.exit(1)

print(f"📄 Файл с ценами: {price_file}")

# ── Чтение Excel ─────────────────────────────────────────────
if price_file.endswith(".csv"):
    df = pd.read_csv(price_file, dtype=str)
else:
    df = pd.read_excel(price_file, header=0, dtype=str)

# Нормализуем названия столбцов
df.columns = [c.strip().lower() for c in df.columns]

# Ищем столбец с идентификатором (EAN / SKU)
id_col = None
for col_name in ["ean", "ean13", "barcode", "kod_ean", "sku", "id"]:
    if col_name in df.columns:
        id_col = col_name
        break

# Ищем столбец с ценой (COG)
price_col = None
for col_name in ["cog", "cena", "price", "cost", "cenazakupu", "cena_zakupu",
                  "koszt", "netto", "цена", "себестоимость"]:
    if col_name in df.columns:
        price_col = col_name
        break

if not id_col:
    print(f"❌ Не найден столбец с идентификатором. Столбцы: {list(df.columns)}")
    print("   Ожидается один из: ean, ean13, barcode, kod_ean, sku, id")
    sys.exit(1)

if not price_col:
    print(f"❌ Не найден столбец с ценой. Столбцы: {list(df.columns)}")
    print("   Ожидается один из: cog, cena, price, cost, koszt, netto")
    sys.exit(1)

print(f"   ID столбец: {id_col}")
print(f"   Цена столбец: {price_col}")

# ── Подготовка ценового словаря ──────────────────────────────
price_map = {}  # id_value → float price
for _, row in df.iterrows():
    raw_id = str(row[id_col]).strip()
    raw_price = str(row[price_col]).strip().replace(",", ".").replace(" ", "")

    # Нормализация EAN (убираем .0 от float)
    if "." in raw_id and raw_id.replace(".", "").isdigit():
        raw_id = raw_id.split(".")[0]

    try:
        price = float(raw_price)
        if price > 0:
            price_map[raw_id] = round(price, 2)
    except (ValueError, TypeError):
        continue

print(f"   Загружено цен: {len(price_map)}")

# ── Загрузка products.json ───────────────────────────────────
products_path = os.path.join(os.path.dirname(__file__) or ".", "products.json")
if not os.path.exists(products_path):
    print("❌ products.json не найден. Запусти сначала fetch_offers.yml")
    sys.exit(1)

with open(products_path, encoding="utf-8") as f:
    pj = json.load(f)

products = pj.get("products", [])
print(f"📦 Товаров в products.json: {len(products)}")

# ── Сопоставление и обновление COG ──────────────────────────
matched_ean = 0
matched_offer = 0
not_found = 0

for p in products:
    ean = str(p.get("ean", "")).strip()
    # Нормализация EAN
    if "." in ean and ean.replace(".", "").isdigit():
        ean = ean.split(".")[0]

    # Пробуем сопоставить по EAN
    if ean in price_map:
        cog_value = price_map[ean]
        if "cog" not in p:
            p["cog"] = {}
        for shop_name in SHOP_NAMES:
            p["cog"][shop_name] = cog_value
        matched_ean += 1
        continue

    # Пробуем сопоставить по offer_id (для каждого магазина)
    found = False
    for shop_name in SHOP_NAMES:
        offer_id = (p.get("offers") or {}).get(shop_name, "")
        if offer_id and offer_id in price_map:
            cog_value = price_map[offer_id]
            if "cog" not in p:
                p["cog"] = {}
            for sn in SHOP_NAMES:
                p["cog"][sn] = cog_value
            matched_offer += 1
            found = True
            break

    if not found:
        not_found += 1

# ── Сохранение ───────────────────────────────────────────────
with open(products_path, "w", encoding="utf-8") as f:
    json.dump(pj, f, ensure_ascii=False, separators=(",", ":"))

print(f"\n{'='*55}")
print(f"✅ Готово!")
print(f"   Совпало по EAN:      {matched_ean}")
print(f"   Совпало по Offer ID: {matched_offer}")
print(f"   Не найдено:          {not_found}")
print(f"   Всего товаров:       {len(products)}")
print(f"{'='*55}")
