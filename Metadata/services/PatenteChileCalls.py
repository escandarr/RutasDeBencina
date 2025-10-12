import requests
import time
import hmac
import hashlib
from bs4 import BeautifulSoup
import base64

headers_get_token = {
    "Host": "www.patentechile.com",
    "Cookie": "__gads=ID=541da39e6946543d:T=1756944184:RT=1760128779:S=ALNI_Ma8eKy_YYqL8utUWJCDdHDrgkF04g; __gpi=UID=00001112406fc213:T=1756944184:RT=1760128779:S=ALNI_Mb1ao1sJWZkC8006ZmXviPzY30sOw; __eoi=ID=a7ba104339efda60:T=1756944184:RT=1760128779:S=AA-AfjZc_e6HWRZK9-d2ugrCjjnw; cf_clearance=t4FhE_kBMtj91O_tQhm2pQHjletGA2K_gcJCtZH_oQY-1760128779-1.2.1.1-NTFsLbYkXs92MOqNFCQYjOrisH0yWbdcQhIqFxLwIFjOf_hH92Zk6yXWPJjH4EMnUNghxPPcgtzftD6MfNvBd6frmfqA3owtZ3kW2b0FwFSw5NSB0_av9mzzmUWxPGnpRQ31zWzTc1OdDvOuhyHLIiuf213bLvxoOE4VbHubB7Ug9_zO.wBG6hbCtRBylwAXgRgmdwewikrqrQjHH2MRtgqKOcHL_JNyxikj_iwHdks; FCNEC=%5B%5B%22AKsRol_YUhrQLCrNabE87xzxeFB20ISCLb9hTz_ZkRF9kLCnOt94WRPjiyOAYLNBqE7C-OYFawf5qs61ZREUuFog4qAW4uQLmBv1L7n0l9CSg4O8kkiBtKjgpH7sNYkygaEQclGkQB5O7gObLKMfYoIJDxFJOuB9mQ%3D%3D%22%5D%5D",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Accept-Language": "es-419,es;q=0.9",
    "Sec-Ch-Ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
    "Content-Type": "application/json",
    "Sec-Ch-Ua-Mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://www.patentechile.com",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://www.patentechile.com/",
    "Accept-Encoding": "gzip, deflate",
    "Priority": "u=1, i",
}


headers_get_results = {
    "Host": "www.patentechile.com",
    "Cookie": "__gads=ID=541da39e6946543d:T=1756944184:RT=1760128779:S=ALNI_Ma8eKy_YYqL8utUWJCDdHDrgkF04g; __gpi=UID=00001112406fc213:T=1756944184:RT=1760128779:S=ALNI_Mb1ao1sJWZkC8006ZmXviPzY30sOw; __eoi=ID=a7ba104339efda60:T=1756944184:RT=1760128779:S=AA-AfjZc_e6HWRZK9-d2ugrCjjnw; cf_clearance=t4FhE_kBMtj91O_tQhm2pQHjletGA2K_gcJCtZH_oQY-1760128779-1.2.1.1-NTFsLbYkXs92MOqNFCQYjOrisH0yWbdcQhIqFxLwIFjOf_hH92Zk6yXWPJjH4EMnUNghxPPcgtzftD6MfNvBd6frmfqA3owtZ3kW2b0FwFSw5NSB0_av9mzzmUWxPGnpRQ31zWzTc1OdDvOuhyHLIiuf213bLvxoOE4VbHubB7Ug9_zO.wBG6hbCtRBylwAXgRgmdwewikrqrQjHH2MRtgqKOcHL_JNyxikj_iwHdks; FCNEC=%5B%5B%22AKsRol_YUhrQLCrNabE87xzxeFB20ISCLb9hTz_ZkRF9kLCnOt94WRPjiyOAYLNBqE7C-OYFawf5qs61ZREUuFog4qAW4uQLmBv1L7n0l9CSg4O8kkiBtKjgpH7sNYkygaEQclGkQB5O7gObLKMfYoIJDxFJOuB9mQ%3D%3D%22%5D%5D",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Origin": "https://www.patentechile.com",
    "Referer": "https://www.patentechile.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}

BASE64_PARTS = [
    'Yjcw', 'ODU3', 'NDEy', 'NGEy', 'NzQy', 'YjVl', 'ZWNh', 'YjQ5', 'OTNh',
    'N2Yz', 'ZTkx', 'NTI4', 'Yjhh', 'OTlk', 'Mzhl', 'MWE2', 'NjNi', 'MmZj',
    'YmU5', 'MmUz', 'OGE4', 'Yw=='
]

SECRET_KEYB64 = "".join(BASE64_PARTS)
SECRET_KEY = base64.b64decode(SECRET_KEYB64)


def _build_token_payload(opt: str, valor: str):
    ts = int(time.time())
    valor_normalized = valor.upper()
    message = f"{opt}|{valor_normalized}|{ts}"

    signature = hmac.new(
        SECRET_KEY, 
        message.encode("utf-8"), 
        hashlib.sha256
    ).hexdigest()

    return {
        "opt": opt,
        "valor": valor_normalized,
        "x": message,
        "z": signature,
    }


def parse_vehicle_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    target_fields = [
        'RUT', 'Nombre', 'Patente', 'Tipo', 'Marca', 'Modelo', 'Año', 
        'Color', 'N° Motor', 'N° Chasis', 'Procedencia', 'Fabricante',
        'Tipo de sello', 'Combustible'
    ]
    
    parsed_data = {}
    results_table = soup.find('table', id='tbl-results')
    
    if not results_table:
        return {}
        
    for row in results_table.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) == 2:
            key = cols[0].get_text(strip=True)
            value = cols[1].get_text(strip=True)
            if key in target_fields:
                parsed_data[key] = value
                
    return parsed_data


def fetch_vehicle_data(patente="GYVB70"):
    try:
        token_url = "https://www.patentechile.com/v3/token"
        token_payload = _build_token_payload(opt="vehiculo", valor=patente)

        token_response = requests.post(token_url, headers=headers_get_token, json=token_payload)
        token_response.raise_for_status()

        token_data = token_response.json()

        if not token_data.get("status", False):
            raise ValueError(f"Token request failed: {token_data.get('mensaje')}")

        jwt_token = (
            token_data.get("jwt")
            or token_data.get("mensaje")
            or token_data.get("data")
        )

        if not jwt_token:
            raise ValueError("Token response did not include token data")

        results_url = "https://www.patentechile.com/resultados"
        form_data = {'q': jwt_token}
        
        results_response = requests.post(results_url, headers=headers_get_results, data=form_data)
        results_response.raise_for_status()
        
        html_content = results_response.text
        
        return html_content

    except (requests.exceptions.RequestException, ValueError):
        return None
    


