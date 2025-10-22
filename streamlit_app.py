import streamlit as st
import pandas as pd
import requests
import time
import re
from typing import Dict, List, Any, Optional

# --- Configuração da Página ---
# Usamos o layout "wide" para que o dashboard ocupe a tela inteira
st.set_page_config(layout="wide")

# --- Funções de Scraping e Limpeza (do nosso script anterior) ---
# (Colocamos todas as funções auxiliares aqui)

def get_holidays_list() -> List[pd.Timestamp]:
    """Busca e processa a lista de feriados da ANBIMA uma única vez."""
    try:
        url = 'https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls'
        df_feriados = pd.read_excel(url)
        df_feriados.dropna(inplace=True)
        # Retorna os dias úteis ANTERIORES aos feriados
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
    
    # Mapeamento de URLs
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
            # Em vez de printar, usamos o logger do streamlit
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

    # Lógica de pivô para séries anuais
    df_pivotado = df.pivot_table(index=df.index.name, columns='DataReferencia', values='Mediana', aggfunc='first')
    
    # Filtra por sextas-feiras OU vésperas de feriado
    filtro_dia = (df_pivotado.index.weekday == 4) | (df_pivotado.index.isin(holiday_list))
    return df_pivotado[filtro_dia]


# --- Funções do App Streamlit ---

@st.cache_data(ttl=3600) # <-- MÁGICA! Cacheia os dados por 1 hora (3600s)
def carregar_dados_focus() -> Dict[str, pd.DataFrame]:
    """
    Função principal que busca e processa TODOS os dados.
    O Streamlit armazena o resultado em cache.
    """
    # Usamos o seu `timeseries_principais` diretamente
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
        'Focus IPCA Serviços Bacen': 'IPCA Serviços'
    }
    
    # Remove as séries que não estão no seu `series_principais`
    lista_series_raw = series_map.keys()
    
    dicionario_dfs = {}
    holidays = get_holidays_list()

    total_series = len(lista_series_raw)
    progress_bar = st.progress(0, text="Buscando dados no BCB...")

    for i, series_name in enumerate(lista_series_raw):
        series_code = series_map[series_name]
        
        json_data = scrap_olinda_requests(series_code, series_name)
        
        if json_data:
            cleaned_df = clean_olinda_requests(json_data, series_name, holidays)
            if not cleaned_df.empty:
                # Remove colunas que não são anos (ex: '2022-01')
                cleaned_df = cleaned_df.loc[:, [col for col in cleaned_df.columns if str(col).isdigit()]]
                dicionario_dfs[series_name] = cleaned_df
        
        progress_bar.progress((i + 1) / total_series, text=f"Buscando: {series_name}")
    
    progress_bar.empty() # Limpa a barra de progresso
    return dicionario_dfs

@st.cache_data
def criar_tabela_resumo(dicionario_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Cria a tabela de resumo principal no estilo da imagem."""
    
    # Pega os anos dinamicamente do primeiro DataFrame
    anos_disponiveis = list(dicionario_dfs.values())[0].columns
    
    # Pega o ano atual e os próximos 3 (total 4 anos)
    ano_atual = pd.Timestamp.now().year
    anos = [str(a) for a in anos_disponiveis if int(a) >= ano_atual and int(a) < ano_atual + 4]
    
    col_tuples = []
    for ano in anos:
        col_tuples.extend([(ano, 'Há 4 semanas'), (ano, 'Hoje'), (ano, 'Comp.')])
    
    nomes_indicadores = [s.replace('Focus ', '').replace(' Bacen', '') for s in dicionario_dfs.keys()]
    
    df_summary = pd.DataFrame(
        index=nomes_indicadores,
        columns=pd.MultiIndex.from_tuples(col_tuples)
    )

    for nome_serie, df in dicionario_dfs.items():
        nome_limpo = nome_serie.replace('Focus ', '').replace(' Bacen', '')
        
        if len(df) < 5:
            st.warning(f"Série '{nome_limpo}' tem menos de 5 observações, pulando tabela resumo.")
            continue

        hoje_vals = df.iloc[-1]
        semana4_vals = df.iloc[-5]
        
        for ano in anos:
            val_hoje = hoje_vals.get(ano, pd.NA)
            val_semana4 = semana4_vals.get(ano, pd.NA)
            
            df_summary.loc[nome_limpo, (ano, 'Há 4 semanas')] = val_semana4
            df_summary.loc[nome_limpo, (ano, 'Hoje')] = val_hoje
            
            arrow = '–'
            if pd.notna(val_hoje) and pd.notna(val_semana4):
                if val_hoje > val_semana4: arrow = '🔺'
                elif val_hoje < val_semana4: arrow = '🔻'
                else: arrow = '🟰'
            df_summary.loc[nome_limpo, (ano, 'Comp.')] = arrow
            
    return df_summary

# --- Layout do App ---

st.title("Dashboard de Expectativas de Mercado - Focus (BCB)")

# 1. Carrega os dados (usando o cache)
dicionario_dfs = carregar_dados_focus()

if not dicionario_dfs:
    st.error("Nenhum dado foi carregado. Verifique a conexão ou a API do BCB.")
else:
    # Mostra a data da última atualização
    last_update_date = list(dicionario_dfs.values())[0].index[-1]
    st.caption(f"Última atualização (data 'Hoje'): {last_update_date.strftime('%d/%m/%Y')}")

    # 2. Cria e exibe a Tabela Resumo
    st.header("Mediana - Agregado")
    
    # Pega os anos da tabela para formatar
    anos_tabela = [str(a) for a in range(pd.Timestamp.now().year, pd.Timestamp.now().year + 4)]
    
    df_resumo = criar_tabela_resumo(dicionario_dfs)
    
    # Formatação para os números e centralização para os ícones
    st.dataframe(
        df_resumo.style
        .format(formatter={(a, 'Há 4 semanas'): '{:.2f}', (a, 'Hoje'): '{:.2f}' for a in anos_tabela}, na_rep="-")
        .set_properties(**{'text-align': 'center'}, subset=[(a, 'Comp.') for a in anos_tabela]),
        use_container_width=True
    )
    st.markdown("---")

    # 3. Cria e exibe os Gráficos Individuais
    st.header("Gráficos Individuais (Evolução das Expectativas)")
    
    df_list = list(dicionario_dfs.items())
    
    # Loop para criar o layout de 2 colunas
    for i in range(0, len(df_list), 2):
        col1, col2 = st.columns(2)
        
        # Gráfico na Coluna 1
        with col1:
            nome1, df1 = df_list[i]
            st.subheader(nome1.replace('Focus ', '').replace(' Bacen', ''))
            
            # Pega apenas os 4 anos principais para o gráfico
            anos_grafico = [col for col in df1.columns if int(col) >= pd.Timestamp.now().year and int(col) < pd.Timestamp.now().year + 4]
            if anos_grafico:
                st.line_chart(df1[anos_grafico])
            else:
                st.line_chart(df1) # Mostra todos se o filtro falhar

        # Gráfico na Coluna 2 (se existir)
        if (i + 1) < len(df_list):
            with col2:
                nome2, df2 = df_list[i+1]
                st.subheader(nome2.replace('Focus ', '').replace(' Bacen', ''))
                
                anos_grafico_2 = [col for col in df2.columns if int(col) >= pd.Timestamp.now().year and int(col) < pd.Timestamp.now().year + 4]
                if anos_grafico_2:
                    st.line_chart(df2[anos_grafico_2])
                else:

                    st.line_chart(df2)
