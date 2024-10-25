import pandas as pd
import requests
import re
from datetime import datetime


informes_urls = [
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/",
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_TRIMESTRAL/DADOS/",
    "https://dados.cvm.gov.br/dados/FII/DOC/INF_ANUAL/DADOS/",
]

informe_data_pattern = r'.+"(inf.+zip)">\1<\/a>.+(\d{2}-\w{3}-\d{4}\s\d{2}:\d{2}).+'

file_url = []
datetime_url = []

for url_base in informes_urls:
    response = requests.get(url_base)

    if response.status_code != 200:
        raise Exception(f"HTTP Error {response.status_code}")

    for line in response.iter_lines():
        strings_match = re.match(informe_data_pattern, line.decode())

        if strings_match:
            file_url.append(f"{url_base}{strings_match.group(1)}")
            datetime_url.append(
                datetime.strptime(strings_match.group(2), "%d-%b-%Y %H:%M")
            )

df = pd.DataFrame({"file_url": file_url, "datetime": datetime_url})

df.to_parquet("sync.pq")
