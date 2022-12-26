import requests
import json
import pygsheets
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta as dt_delta

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjp7ImlkIjozMDIxNjA2NjAsImVtYWlsIjoiZW1hbnVlbC5oYXJrQGltZWpyLmNvbSIsImFwcGxpY2F0aW9uIjozMDAxOTYxMjN9fQ.7roG2Slj15liyo1u7h_EXKr12TIjMPqC0fLXVlSRi-QYE3mZdwy5UI7RbrWpHgJArsTaV3h4cBjC8or5kEJ3Bg"
URL = 'nps-insta-credentials.json'
def base(token):
  token = token
  token_real = "Bearer " + token
  url = "https://api.pipefy.com/graphql"
  pipe_id = "302206448"
  payload_4 = {"query": "{ allCards (pipeId: 302206448) { edges { node {id title phases_history { phase { name } firstTimeIn lastTimeIn lastTimeOut duration}}}} }"} # id-Cards
  headers = {
      "accept": "application/json",
      "Authorization": token_real,
      "content-type": "application/json"
  }
  response = requests.post(url,json=payload_4, headers=headers)
  json_response_all_cards = json.loads(response.text)
  return json_response_all_cards

def base_update(token,data):
  token = token
  data_maxima = str(pd.to_datetime(maximum_time(data)))
  data_maximas = data_maxima[0:10] + "T" + data_maxima[11:25]
  data_maximila = "2022-12-04T12:44:12+00:00"
  token_real = "Bearer " + token
  url = "https://api.pipefy.com/graphql"
  pipe_id = "302206448"
  payload_5 = {"query": "{ allCards (pipeId: 302206448, filter: {field:\"updated_at\",operator:gt,value:\"" + data_maximas + "\"}) { edges { node {id,title,due_date,phases_history { phase { name } firstTimeIn lastTimeIn lastTimeOut duration}}}} }"}
  headers = {
      "accept": "application/json",
      "Authorization": token_real,
      "content-type": "application/json"
  }
  response = requests.post(url,json=payload_5, headers=headers)
  json_response_all_cards = json.loads(response.text)
  return json_response_all_cards 

def create(json_response_all_cards):
  data = json_response_all_cards['data']['allCards']['edges']
  empresa = []
  etapa = []
  etapa_inicio = []
  etapa_fim = []
  id = []
  duracao = []
  turnover_step = []
  convert_step = []
  for i in range(0,len(data)):
    data_user = data[i]['node']
    phases = data_user['phases_history']
    id_spec = data_user['id']
    for j in range(1,len(phases)):
      empresa.append(data_user['title'])
      unique = phases[j]
      etapa.append(unique['phase']['name'])
      etapa_inicio.append(unique['firstTimeIn'])
      etapa_fim.append(unique['lastTimeOut'])
      id.append(id_spec)
      duracao.append(unique['duration'])
      if(unique['phase']['name'] == 'Lead Perdido'):
        turnover_step.append(phases[j-1]['phase']['name'])
      else:
        turnover_step.append('Sucess')
      if(j < len(phases) - 1):
        if(phases[j+1]['phase']['name'] != 'Lead Perdido'):
          convert_step.append(phases[j]['phase']['name'])
        else:
          convert_step.append('Failed')
      else:
        convert_step.append('Failed')
  df = pd.DataFrame()
  # Create a column
  df['id_card'] = id
  df['empresa'] = empresa
  df["etapa"] = etapa
  df["etapa_inicio"] = etapa_inicio
  et_st = pd.to_datetime(pd.Series(df['etapa_inicio'])).dt.date
  df['tempo_inicio'] = et_st
  df['etapa_fim'] = etapa_fim
  et_end = pd.to_datetime(df['etapa_fim'].fillna(dt.today())).dt.date
  df['tempo_fim'] = et_end
  df['duracao'] = duracao
  days, remainder = divmod(df['duracao'], 60 * 60 * 24)
  df['et_diff'] = days + 1
  df["turnover_etapa"] = turnover_step
  df['convert_etapa'] = convert_step
  df.loc[(df['etapa'] == 'Fechamento de Contrato') | (df['etapa'] == 'Lead Perdido'), 'et_diff'] = 0
  write(URL,df)
  return df

def treatment_time(dados):
  et_st = pd.to_datetime(dados['etapa_inicio'])
  dados['finish'] = np.where(dados['etapa_fim'].isnull(),False,True)
  et_end_tw = pd.to_datetime(dados['etapa_fim'].fillna(dt.today()))
  dados['et_diff'] = (et_end_tw - et_st).dt.days + 1
  dados.loc[(dados['etapa'] == 'Fechamento de Contrato') | (dados['etapa'] == 'Lead Perdido'), 'et_diff'] = 0
  return dados

def treatment_business(dados):
  dados_bus = dados[(dados['etapa'] == 'Negociação' ) & (pd.isna(dados['etapa_fim']))]['empresa']
  qwe = dados.loc[(dados['empresa'].isin(dados_bus)) & (dados['etapa'] == 'Fechamento de Contrato'),'etapa_inicio']
  empresa = dados.loc[(dados['empresa'].isin(dados_bus)) & (dados['etapa'] == 'Fechamento de Contrato'), 'empresa']
  dados['etapa_fim'][(dados['empresa'] == empresa.iloc[0]) & (dados['etapa'] == 'Negociação')] = qwe.iloc[0]
  etapa_fim = dados.loc[(dados['empresa'] == empresa.iloc[0]) & (dados['etapa'] == 'Negociação'),'etapa_fim']
  etapa_inicio = dados.loc[(dados['empresa'] == empresa.iloc[0]) & (dados['etapa'] == 'Negociação'),'etapa_inicio']
  diff = pd.to_datetime(etapa_fim) - pd.to_datetime(etapa_inicio)
  et_diff = (diff).dt.days
  dados['et_diff'][(dados['empresa'] == empresa.iloc[0]) & (dados['etapa'] == 'Negociação')] = et_diff
  return dados

def update(url,token):
  dados = read(url)
  json_response_all_cards = base_update(token,dados)
  data = json_response_all_cards['data']['allCards']['edges']
  empresa = []
  etapa = []
  etapa_inicio = []
  etapa_fim = []
  id = []
  duration = []
  turnover_step = []
  convert_step = []
  if(len(data) != 0):
    for i in range(0,len(data)):
      data_user = data[i]['node']
      phases = data_user['phases_history']
      id_spec = data_user['id']
      due_date = data_user['due_date']
      for j in range(1,len(phases)):
        empresa.append(data_user['title'])
        unique = phases[j]
        etapa.append(unique['phase']['name'])
        etapa_inicio.append(unique['firstTimeIn'])
        etapa_fim.append(unique['lastTimeOut'])
        id.append(id_spec)
        duration.append(unique['duration'])
        if(unique['phase']['name'] == 'Lead Perdido'):
          turnover_step.append(phases[j-1]['phase']['name'])
        else:
          turnover_step.append('Sucess')
        if(j < len(phases) - 1):
          if(phases[j+1]['phase']['name'] != 'Lead Perdido'):
            convert_step.append(phases[j]['phase']['name'])
          else:
            convert_step.append('Failed')
        else:
          convert_step.append('Failed')
    df = pd.DataFrame()
    # Create a column
    df['id_card'] = id
    df['empresa'] = empresa
    df["etapa"] = etapa
    df["etapa_inicio"] = etapa_inicio
    et_st = pd.to_datetime(pd.Series(df['etapa_inicio'])).dt.date
    df['tempo_inicio'] = et_st
    df['etapa_fim'] = etapa_fim
    et_end = pd.to_datetime(df['etapa_fim'].fillna(dt.today())).dt.date
    df['tempo_fim'] = et_end
    df['duracao'] = duration
    days, remainder = divmod(df['duracao'], 60 * 60 * 24)
    df['et_diff'] = days + 1
    df["turnover_etapa"] = turnover_step
    df['convert_etapa'] = convert_step
    df.loc[(df['etapa'] == 'Fechamento de Contrato') | (df['etapa'] == 'Lead Perdido'), 'et_diff'] = 0
  else:
    df = pd.DataFrame()
    # Create a column
    df['id_card'] = id
    df['empresa'] = empresa
    df["etapa"] = etapa
    df["etapa_inicio"] = etapa_inicio
    df['tempo_inicio'] = etapa_inicio
    df['etapa_fim'] = etapa_fim
    df['tempo_fim'] = etapa_fim
    df['duracao'] = duration
    df['et_diff'] = []
    df["turnover_etapa"] = turnover_step
    df['convert_etapa'] = convert_step
  return df
def maximum_time(dados):
  etapa_fim = dados['etapa_fim']
  etapa_fim_notnull = etapa_fim.dropna(how='any',axis=0) 
  et_st = pd.to_datetime(dados['etapa_inicio'])
  et_ed = pd.to_datetime(etapa_fim_notnull)
  etapa_inicio = max(et_st)
  etapa_fim = max(et_ed)
  maximum = max(etapa_inicio,etapa_fim)
  return maximum
  
def write(url,data):
  gc = pygsheets.authorize(service_file=url)
  sheet_data = gc.sheet.get('1Y4LTFODfoyiiVoCO3iaB0ot92BgjNOTBg1ldkPo0wvc')
  #open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
  sh = gc.open('Dashboard-Pipefy')
  #select the first sheet 
  wks = sh[0]
  #update the first sheet with df, starting at cell B2. 
  wks.set_dataframe(data,(1,1))
  return 2

  
def write_update(url,df):
  data_past = read(url)
  print(data_past.dtypes)
  print(df.head())
  if(len(df) != 0):
    empresas = df['empresa'].unique()
    "data_pres = data_past[~data_past['empresa'].isin(empresas)]"
    filtering = data_past['empresa'].map(lambda x: x not in empresas)
    data_pres = data_past[filtering]
    print(data_pres.head())
    frame = [data_pres,df]
    data_futu = pd.concat(frame)
    write(url,data_futu)
    
    return 2
  return 2

def read(url):
  gc = pygsheets.authorize(service_file=url)
  sheet_data = gc.sheet.get('1Y4LTFODfoyiiVoCO3iaB0ot92BgjNOTBg1ldkPo0wvc')
  #open the google spreadsheet (where 'PY to Gsheet Test' is the name of my sheet)
  sh = gc.open('Dashboard-Pipefy')
  #select the first sheet 
  wks = sh[0]
  return pd.DataFrame(wks.get_all_records()) 
if __name__ == '__main__':
    """
    print(qwe)
    call_api = base(TOKEN)
    ds = create(call_api)
    ad = update(URL,TOKEN)
    qwe = write_update(URL,ad)
    """
    gc = pygsheets.authorize(service_file=URL)
    sheet_data = gc.open_by_key('1Y4LTFODfoyiiVoCO3iaB0ot92BgjNOTBg1ldkPo0wvc')
    sh = sheet_data.sheet1
    print(sh.get_all_records())