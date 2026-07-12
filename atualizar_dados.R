# ==============================================================================
# PIPELINE ETL: ATUALIZAÇÃO DA BALANÇA COMERCIAL
# Descrição: Script responsável por conectar ao Google Cloud, extrair o 
#            histórico de importações/exportações, processar strings e 
#            salvar os dados otimizados no formato .parquet localmente.
# ==============================================================================

library(basedosdados)
library(dplyr)
library(arrow)

# 1. Conectando ao projeto no Google Cloud
set_billing_id("automacao-dashboard-502016")

print("Iniciando rotina de extração e processamento de dados...")

# 2. Consulta para EXPORTAÇÕES (Com SPLIT para resumir as nomenclaturas dos produtos)
query_exportacao <- "
  SELECT 
    t.ano,
    t.mes,
    uf.nome AS no_uf,
    uf.regiao AS no_regiao,
    pais.nome_pt AS no_pais,
    COALESCE(
      TRIM(SPLIT(dic.nome_ncm_portugues, ',')[OFFSET(0)]), 
      CONCAT('NCM ', CAST(t.id_ncm AS STRING))
    ) AS no_cuci_grupo,
    SUM(t.peso_liquido_kg) / 1000000000 AS peso_liquido_kg,
    SUM(t.valor_fob_dolar) / 1000000 AS valor_fob_dolar
  FROM `basedosdados.br_me_comex_stat.ncm_exportacao` AS t
  LEFT JOIN `basedosdados.br_bd_diretorios_brasil.uf` AS uf
    ON t.sigla_uf_ncm = uf.sigla
  LEFT JOIN `basedosdados.br_bd_diretorios_mundo.pais` AS pais
    ON t.sigla_pais_iso3 = pais.sigla_iso3
  LEFT JOIN `basedosdados.br_bd_diretorios_mundo.nomenclatura_comum_mercosul` AS dic
    ON CAST(t.id_ncm AS STRING) = CAST(dic.id_ncm AS STRING)
  WHERE t.ano >= 2014
  GROUP BY t.ano, t.mes, no_uf, no_regiao, no_pais, no_cuci_grupo
"

# 3. Consulta para IMPORTAÇÕES (Com SPLIT para resumir as nomenclaturas dos produtos)
query_importacao <- "
  SELECT 
    t.ano,
    t.mes,
    uf.nome AS no_uf,
    uf.regiao AS no_regiao,
    pais.nome_pt AS no_pais,
    COALESCE(
      TRIM(SPLIT(dic.nome_ncm_portugues, ',')[OFFSET(0)]), 
      CONCAT('NCM ', CAST(t.id_ncm AS STRING))
    ) AS no_cuci_grupo,
    SUM(t.peso_liquido_kg) / 1000000000 AS peso_liquido_kg,
    SUM(t.valor_fob_dolar) / 1000000 AS valor_fob_dolar
  FROM `basedosdados.br_me_comex_stat.ncm_importacao` AS t
  LEFT JOIN `basedosdados.br_bd_diretorios_brasil.uf` AS uf
    ON t.sigla_uf_ncm = uf.sigla
  LEFT JOIN `basedosdados.br_bd_diretorios_mundo.pais` AS pais
    ON t.sigla_pais_iso3 = pais.sigla_iso3
  LEFT JOIN `basedosdados.br_bd_diretorios_mundo.nomenclatura_comum_mercosul` AS dic
    ON CAST(t.id_ncm AS STRING) = CAST(dic.id_ncm AS STRING)
  WHERE t.ano >= 2014
  GROUP BY t.ano, t.mes, no_uf, no_regiao, no_pais, no_cuci_grupo
"

# 4. Executando as consultas
print("Baixando dados de Exportação ")
dados_exp <- read_sql(query_exportacao)

print("Baixando dados de Importação ")
dados_imp <- read_sql(query_importacao)

# 5. Transformação final
meses_pt <- c("jan.", "fev.", "mar.", "abr.", "maio", "jun.", 
              "jul.", "ago.", "set.", "out.", "nov.", "dez.")

dados_exp <- dados_exp |> 
    mutate(nome_mes = meses_pt[as.integer(mes)]) |> 
    filter(!is.na(no_uf))

dados_imp <- dados_imp |> 
    mutate(nome_mes = meses_pt[as.integer(mes)]) |> 
    filter(!is.na(no_uf))

# 6. Salvar os arquivos
if (!dir.exists("dados")) {
    dir.create("dados")
}

print("Salvando arquivos na pasta local 'dados/'...")
write_parquet(dados_exp, "dados/ncm_exportacao_agrupado.parquet")
write_parquet(dados_imp, "dados/ncm_importacao_agrupado.parquet")

print("Processamento concluído. Arquivos atualizados com sucesso.")