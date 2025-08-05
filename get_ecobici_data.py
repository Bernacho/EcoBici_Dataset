import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import pytz
import pandas as pd
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq

from upload_to_gcs import upload_partitioned_dataset_skip_existing, file_exists_in_gcs


tz = pytz.timezone("America/Mexico_City")
now = datetime.now(tz=tz)

URL = 'https://ecobici.cdmx.gob.mx/datos-abiertos/'
DATA_PATH = "partitioned_historical_data"

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)

def process_historical_data(file_url):
    file_response = requests.get(file_url)
    csv_content = file_response.content.decode('utf-8')
    
    dict_types = {"Genero_Usuario":"str","Edad_Usuario":"Int64","Bici":"str","Ciclo_Estacion_Retiro":"str",
                                        "Hora_Retiro":"str","Ciclo_Estacion_Arribo":"str","Hora_Arribo":"str"}
    
    data = pd.read_csv(StringIO(csv_content),dtype=dict_types)
    if "Hora_Retiro.1" in data.columns:
        data.rename(columns={"Hora_Retiro.1":"Hora_Arribo"},inplace=True)
    known_col_errors = {"EstacionArribo":"Estacion_Arribo",
                        "CE":"Ciclo_Estacion",
                        "retiro":"Retiro",
                        "arribo":"Arribo",
                       "usuario":"Usuario"}
    data.columns = [x.replace(" ","_") for x in data.columns]
    for e,s in known_col_errors.items():
        data.columns = [x.replace(e,s) for x in data.columns]
    for c,ty in dict_types.items():
        data[c] = data[c].astype(ty)
    
    date_format = "%d/%m/%Y"
    if pd.to_datetime(data.Fecha_Retiro,format=date_format,errors='coerce').notna().sum()==0:
        date_format = "%d/%m/%y"
    data['Fecha_Retiro'] = pd.to_datetime(data.Fecha_Retiro,format=date_format,errors='coerce')
    data['Fecha_Arribo'] = pd.to_datetime(data.Fecha_Arribo,format=date_format,errors='coerce')
    data['date_start'] = pd.to_datetime(data.Fecha_Retiro.dt.strftime("%m/%d/%Y")+" "+data.Hora_Retiro, errors="coerce")
    data['date_end'] = pd.to_datetime(data.Fecha_Arribo.dt.strftime("%m/%d/%Y") +" "+data.Hora_Arribo, errors="coerce")
    data.dropna(subset=['Edad_Usuario','Ciclo_Estacion_Retiro','Ciclo_Estacion_Arribo',
                        'Fecha_Retiro','Fecha_Arribo','date_start','date_end'],inplace=True)
    data['duration'] = (data.date_end - data.date_start).dt.seconds
    file_name = file_url.split("/")[-1]
    data['file'] = file_name

    y,m = file_name.split(".")[0].split("-")
    data['year'] = int(y)
    data['month'] = int(m)
    start_date = datetime(year=int(y),month=int(m), day=1)
    end_date = last_day_of_month(start_date)

    assert data.Fecha_Arribo.min().date()==start_date.date(), (data.Fecha_Arribo.min().date(),start_date)
    assert data.Fecha_Arribo.max().date()==end_date.date(), (data.Fecha_Arribo.max().date(),end_date)
    
    return data[['Genero_Usuario', 'Edad_Usuario', 'Bici', 'Ciclo_Estacion_Retiro',
       'Fecha_Retiro', 'Hora_Retiro', 'Ciclo_Estacion_Arribo', 'Fecha_Arribo',
       'Hora_Arribo', 'date_start', 'date_end', 'duration', 'file','year','month']]


month_end = now.replace(day=1)- timedelta(days=1)

link_text = f"{month_end.year}-{('' if month_end.month>9 else '0')+str(month_end.month)}"

if file_exists_in_gcs(link_text)==False:

    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    link = soup.find('a', string=lambda text: text and link_text in text,href=lambda href: href and '.csv' in href)

    if link and link['href']:
        file_url = link['href']
        if not file_url.startswith('http'):
            file_url = requests.compat.urljoin(URL, file_url)

        
        df = process_historical_data(file_url)
        print(df.shape)
        print(df['file'].iloc[0])
        # save file if it is not part of the historical data
        print(f"Saving file {link_text}")
        print(df.columns)
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=DATA_PATH,
            partition_cols=['year','month']
            )            

        print("File saved!")

        ## Upload files to GCS
        upload_partitioned_dataset_skip_existing(local_root=DATA_PATH)
    else:
        print(f"File for {link_text} not found")
else:
    print(f"File {link_text} is already in the dataset")