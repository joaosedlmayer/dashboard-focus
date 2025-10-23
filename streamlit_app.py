# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 14:44:04 2025

@author: joaos
"""

import streamlit as st
import pandas as pd
import requests
import time
import re
from typing import Dict, List, Any, Optional
import altair as alt  # <-- ADICIONADO AQUI

# --- Configuração da Página ---
st.set_page_config(layout="wide")

# --- Funções de Scraping e Limpeza (Inalteradas) ---

@st.cache_data(ttl=3600)
def get_holidays_list() -> List[pd.Timestamp]:
    """Busca e processa a lista de feriados da ANBIMA uma única vez."""
    try:
        url = 'https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls'
        df_feriados = pd.read_excel(url)
        df_feriados.dropna(inplace=True)
        return [d - pd.tseries.offsets.BDay(1) for d in pd.to_datetime(df_feriados['Data'], dayfirst=True)]
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível buscar a lista de feriados: {e}.")
        return []

def scrap_olinda_requests(codigo: str, series_name: str) -> Optional[Dict[str, Any]]:
    """Busca dados da API Olinda com retentativas."""
    coords = None
    if ";" in str(codigo):
        parts = str(codigo).split(";")
        codigo = parts[0]
        coords = parts[1]
    
    tries = 5
    url = ""
    
    if series_name == 'Focus Curva Selic':
        url = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoSelic?$top=10000&$filter=Data%20ge%20%272022-01-07%27%20and%20baseCalculo%20eq%200&$format=json&$select=Data,Reuniao,Mediana,baseCalculo"
    elif 'Média' in series_name:
        url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$filter=Indicador%20eq%20'{codigo}'%20and%20Data%20ge%20'2022-01-07'&$format=json&$select=Indicador,Data,DataReferencia,Media"
    elif series_name == 'Focus Balança Comercial Bacen':
        url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$filter=Indicador%20eq%20'Balan%C3%A7a%20comercial'%20and%20IndicadorDetalhe%20eq%20'{coords}'%20and%20Data%20ge%20'2022-01-07'&$format=json&$select=Indicador,Data,DataReferencia,Mediana"
    elif '5 dias' in series_name:
        url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$filter=Indicador%20eq%20'{codigo}'%20and%20baseCalculo%20eq%201%20and%20Data%20ge%20'2022-01-07'&$format=json"
    elif '(M)' in series_name:
        url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativaMercadoMensais?$filter=Indicador%20eq%20'{codigo}'%20and%20Data%20ge%20'2022-01-07'%20and%20baseCalculo%20eq%200&$format=json&$select=Indicador,Data,DataReferencia,Mediana,baseCalculo"
    else:
        url = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoAnuais?$filter=Indicador%20eq%20'{codigo}'%20and%20Data%20ge%20'2022-01-07'&$format=json&$select=Indicador,Data,DataReferencia,Mediana"

    for i in range(tries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            st.warning(f"Tentativa {i + 1} falhou para '{series_name}': {e}", icon="⚠️")
            time.sleep(1)
    
    st.error(f"Falha ao obter dados para '{series_name}' após {tries} tentativas.", icon="❌")
    return None

def clean_olinda_requests(json_data: Dict[str, Any], series_name: str, holiday_list: List[pd.Timestamp]) -> pd.DataFrame:
    """Limpa o JSON da API e transforma em DataFrame."""
    if not json_data or 'value' not in json_data or not json_data['value']:
        return pd.DataFrame()

    df = pd.DataFrame(json_data['value'])
    df['Data'] = pd.to_datetime(df['Data'])
    df.set_index('Data', inplace=True)
    
    if '(M)' in series_name:
        df.drop(columns=['Indicador', 'baseCalculo'], inplace=True, errors='ignore')
        df['DataReferencia'] = pd.to_datetime(df['DataReferencia'])
        return df.pivot_table(index=df.index.name, columns='DataReferencia', values='Mediana', aggfunc='first')

    df_pivotado = df.pivot_table(index=df.index.name, columns='DataReferencia', values='Mediana', aggfunc='first')
    filtro_dia = (df_pivotado.index.weekday == 4) | (df_pivotado.index.isin(holiday_list))
    return df_pivotado[filtro_dia]


# --- Funções do App Streamlit ---

@st.cache_data(ttl=3600)
def carregar_dados_focus() -> Dict[str, pd.DataFrame]:
    """Função principal que busca e processa TODOS os dados."""
    series_map = {
        'Focus IPCA Bacen': 'IPCA',
        'Focus IPCA Bacen 5 dias': 'IPCA',
        'Focus PIB Bacen': 'PIB Total',
        'Focus Selic Bacen': 'Selic',
        'Focus Câmbio Bacen': 'Câmbio',
        'Focus IGP-M Bacen': 'IGP-M',
        'Focus IPCA Administrados Bacen': 'IPCA Administrados',
        'Focus Conta corrente Bacen': 'Conta corrente',
        'Focus Balança Comercial Bacen': 'Balança comercial;Saldo',
        'Focus Investimento direto no país Bacen': 'Investimento direto no país',
        'Focus Dívida líquida do setor público Bacen': 'Dívida líquida do setor público',
        'Focus Resultado nominal Bacen': 'Resultado nominal',
        'Focus Resultado primário Bacen': 'Resultado primário',
        'Focus IPCA Alimentação no domicílio Bacen': 'IPCA Alimentação no domicílio',
        'Focus IPCA Bens industrializados Bacen': 'IPCA Bens industrializados',
        'Focus IP
