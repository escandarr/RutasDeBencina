import requests
import json
from bs4 import BeautifulSoup


headers_get_token = {
    "Host": "www.patentechile.com",
    "Cookie": "cf_clearance=2ZmWxaX1u6Mq5ed.EOdLPi26kNVor5oTLolIbA8a2Mc-1756944183-1.2.1.1-q7eCO_17lGtiQLM3MYVO8dmU.FVHOhOD2DZCRyxmcwJ4guqCUECq_l.zFHf6szdyHoeBniJdruvEVHBg0OnQWa6p70fDTTJvY6DM_8Iz3914G7MQi7UnPqv5mJ7GbD2QxYZ44q4n0idTCTWNVCkrIm9xX6H760ufLnhfCwFVsLjpJgHlIKyL1pPfZ6.AWYQ2wEq4Gb3nxQXT2zaQQg1fAKuLEbBwdssv_dM2hJaZeaU; __gads=ID=541da39e6946543d:T=1756944184:RT=1756944184:S=ALNI_Ma8eKy_YYqL8utUWJCDdHDrgkF04g; __gpi=UID=00001112406fc213:T=1756944184:RT=1756944184:S=ALNI_Mb1ao1sJWZkC8006ZmXviPzY30sOw; __eoi=ID=a7ba104339efda60:T=1756944184:RT=1756944184:S=AA-AfjZc_e6HWRZK9-d2ugrCjjnw; FCNEC=%5B%5B%22AKsRol8WJkxjRKaCRUkFzMVLy7XwgoG3MdpBE8z_IDmdurxfiyLGmeBUtScKufT56Eq-sIDKzXxwPp4yu66iJXeCaGz-ZrrAoiWyBm6BKyYCelmKu2zzuDjf3gWfcbwwE-xpy80tLyXxFWd7Mdt9mrOgut1ZsjVamQ%3D%3D%22%5D%5D",
    "Content-Type": "text/plain;charset=UTF-8",
    "Accept": "*/*",
    "Origin": "https://www.patentechile.com",
    "Referer": "https://www.patentechile.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
}


headers_get_results = {
    "Host": "www.patentechile.com",
    "Cookie": "cf_clearance=2ZmWxaX1u6Mq5ed.EOdLPi26kNVor5oTLolIbA8a2Mc-1756944183-1.2.1.1-q7eCO_17lGtiQLM3MYVO8dmU.FVHOhOD2DZCRyxmcwJ4guqCUECq_l.zFHf6szdyHoeBniJdruvEVHBg0OnQWa6p70fDTTJvY6DM_8Iz3914G7MQi7UnPqv5mJ7GbD2QxYZ44q4n0idTCTWNVCkrIm9xX6H760ufLnhfCwFVsLjpJgHlIKyL1pPfZ6.AWYQ2wEq4Gb3nxQXT2zaQQg1fAKuLEbBwdssv_dM2hJaZeaU; __gads=ID=541da39e6946543d:T=1756944184:RT=1756944184:S=ALNI_Ma8eKy_YYqL8utUWJCDdHDrgkF04g; __gpi=UID=00001112406fc213:T=1756944184:RT=1756944184:S=ALNI_Mb1ao1sJWZkC8006ZmXviPzY30sOw; __eoi=ID=a7ba104339efda60:T=1756944184:RT=1756944184:S=AA-AfjZc_e6HWRZK9-d2ugrCjjnw; FCNEC=%5B%5B%22AKsRol8WJkxjRKaCRUkFzMVLy7XwgoG3MdpBE8z_IDmdurxfiyLGmeBUtScKufT56Eq-sIDKzXxwPp4yu66iJXeCaGz-ZrrAoiWyBm6BKyYCelmKu2zzuDjf3gWfcbwwE-xpy80tLyXxFWd7Mdt9mrOgut1ZsjVamQ%3D%3D%22%5D%5D",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Origin": "https://www.patentechile.com",
    "Referer": "https://www.patentechile.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
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
        token_url = "https://www.patentechile.com/web-app0001/controller/createopt2"
        token_payload = {
            "tk": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZXJtIjoiZ2V0X3RrIn0.sv0H3BIMLI6sPLao5cAbKkDUwhqS8-Zx-fC6UFjk3AU",
            "opt": "vehiculo",
            "t": patente
        }
        
        token_response = requests.post(token_url, headers=headers_get_token, data=json.dumps(token_payload))
        token_response.raise_for_status()
        jwt_token = token_response.text 

        results_url = "https://www.patentechile.com/resultados"
        form_data = {'q': jwt_token}
        
        results_response = requests.post(results_url, headers=headers_get_results, data=form_data)
        results_response.raise_for_status()
        
        html_content = results_response.text
        
        return html_content

    except requests.exceptions.RequestException as e:
        return None
    


