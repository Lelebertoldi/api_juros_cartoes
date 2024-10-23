from IPython.display import display
import requests
import pandas as pd


# URL das API de 2023
trimestre_1 = "https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/TaxasJurosDiariaPorInicioPeriodo?$format=json&$filter=InicioPeriodo ge '2023-01-01' and FimPeriodo le '2023-03-31'&$orderby=InicioPeriodo asc"
trimestre_2 = "https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/TaxasJurosDiariaPorInicioPeriodo?$format=json&$filter=InicioPeriodo ge '2023-04-01' and FimPeriodo le '2023-06-30'&$orderby=InicioPeriodo asc"
trimestre_3 = "https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/TaxasJurosDiariaPorInicioPeriodo?$format=json&$filter=InicioPeriodo ge '2023-07-01' and FimPeriodo le '2023-09-30'&$orderby=InicioPeriodo asc"
trimestre_4 = "https://olinda.bcb.gov.br/olinda/servico/taxaJuros/versao/v2/odata/TaxasJurosDiariaPorInicioPeriodo?$format=json&$filter=InicioPeriodo ge '2023-10-01' and FimPeriodo le '2023-12-31'&$orderby=InicioPeriodo asc"


def request(url):
    # Fazer a requisição à API
    response = requests.get(url)

    # Verificar se a requisição de dados foi bem-sucedida
    if response.status_code == 200:
        data = response.json()['value']  # Extrair os dados
        df = pd.DataFrame(data)  # Converter os dados em DataFrame
        print("Dados recebidos com sucesso")
        return df  # Retornar o DataFrame para ser salvo em uma variável
    else:
        print("Erro ao obter os dados:", response.status_code)


# Filtra o DataFrame para incluir apenas as modalidades de cartão de crédito
def filtro_modalidade(DataFrame):
    modalidade_cartao = [
        'Cartão de crédito - rotativo total - Pré-fixado',
        'Cartão de crédito - parcelado - Pré-fixado',
        'Antecipação de faturas de cartão de crédito - Pré-fixado'
    ]

    df_filtrado = DataFrame[DataFrame['Modalidade'].isin(modalidade_cartao)]

    # Retorna o DataFrame filtrado
    return df_filtrado


def main():

    df_trimestre_1 = request(trimestre_1)

    df_trimestre_2 = request(trimestre_2)

    df_trimestre_3 = request(trimestre_3)

    df_trimestre_4 = request(trimestre_4)

    filtro_trimestre_1 = filtro_modalidade(df_trimestre_1)

    filtro_trimestre_2 = filtro_modalidade(df_trimestre_2)

    filtro_trimestre_3 = filtro_modalidade(df_trimestre_3)

    filtro_trimestre_4 = filtro_modalidade(df_trimestre_4)


    df_concatenado = pd.concat([filtro_trimestre_1, filtro_trimestre_2, filtro_trimestre_3, filtro_trimestre_4], ignore_index=True)


    # Limpeza de colunas
    colunas_desejadas = [
        'InicioPeriodo', 
        'FimPeriodo', 
        'Segmento', 
        'Modalidade', 
        'Posicao', 
        'InstituicaoFinanceira', 
        'TaxaJurosAoMes', 
        'TaxaJurosAoAno'
    ]
    # Aplica a limpeza das colunas
    df_final = df_concatenado[colunas_desejadas]
    # Remove nulos
    df_final = df_final.dropna()
    # Ordenar o DataFrame df_final pela coluna 'TaxaJurosAoMes' do menor para o maior
    df_final = df_final.sort_values(by='TaxaJurosAoMes', ascending=True)
    # Exibir o DataFrame final
    display(df_final)


    try:
        # Salvar o DataFrame concatenado no formato Parquet
        df_final.to_parquet('juros_cartoes_oficial.parquet', index=False)
        print("Dados salvos com sucesso no formato Parquet!")
    except Exception as e:
        print("Erro ao salvar os dados em Parquet:", e)


if __name__ == "__main__":
    main()