import pandas as pd
import zipfile
import requests
import re
import os
import io

from datetime import datetime


informes_urls = [
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/",
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/",
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_ANUAL/DADOS/",
]

informe_data_pattern = r'.+"(inf.+zip)">\1<\/a>.+(\d{2}-\w{3}-\d{4}\s\d{2}:\d{2}).+'

file_url = []
datetime_url = []
download_file = []

for url_base in informes_urls:
    response = requests.get(url_base)

    if response.status_code != 200:
        raise Exception(f"HTTP Error {response.status_code}")

    for line in response.iter_lines():
        strings_match = re.match(informe_data_pattern, line.decode())

        if strings_match:
            url = f"{url_base}{strings_match.group(1)}"
            data = datetime.strptime(strings_match.group(2), "%d-%b-%Y %H:%M")
            download = True

            if os.path.exists("sync.pq"):
                df_old = pd.read_parquet("sync.pq")
                df_old.set_index("file_url", inplace=True)

                if df_old.loc[url, "datetime"] >= data:
                    download = False

            file_url.append(url)
            datetime_url.append(data)
            download_file.append(download)

df = pd.DataFrame(
    {"file_url": file_url, "datetime": datetime_url, "download": download_file}
)

inf_path_pattern = r"inf_(\w+)_fii_(.+)_\d{4}\.csv"

for link in df.loc[df["download"] == True, "file_url"]:
    response = requests.get(link, stream=True)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    for file in zip_file.namelist():
        string_match = re.match(inf_path_pattern, file)
        zip_file.extract(
            file, f"./data/raw/{string_match.group(1)}/{string_match.group(2)}/"
        )


df["download"] = False
df.to_parquet("sync.pq")
