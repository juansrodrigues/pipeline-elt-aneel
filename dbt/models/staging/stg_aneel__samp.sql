with source as (
    select * from {{ source('aneel_raw', 'samp_raw') }}
),

renamed as (
    select
        -- datas
        try_to_date(dat_geracao_conjunto_dados)     as dat_geracao_conjunto_dados,
        try_to_date(dat_competencia)                as dat_competencia,

        -- distribuidora
        trim(num_cnpj_agente_distribuidora)         as cnpj_distribuidora,
        upper(trim(sig_agente_distribuidora))       as sigla_distribuidora,
        upper(trim(nom_agente_distribuidora))       as nome_distribuidora,

        -- classificação tarifária
        trim(nom_tipo_mercado)                      as tipo_mercado,
        trim(dsc_modalidade_tarifaria)              as modalidade_tarifaria,
        trim(dsc_sub_grupo_tarifario)               as subgrupo_tarifario,
        trim(dsc_classe_consumo_mercado)            as classe_consumo,
        trim(dsc_sub_classe_consumidor)             as subclasse_consumidor,
        trim(dsc_detalhe_consumidor)                as detalhe_consumidor,

        -- agente acessante (GD / geração distribuída)
        trim(ide_agente_acessante)                  as id_agente_acessante,
        trim(num_cnpj_agente_acessante)             as cnpj_agente_acessante,
        trim(nom_agente_acessante)                  as nome_agente_acessante,

        -- posto e opção
        trim(dsc_posto_tarifario)                   as posto_tarifario,
        trim(dsc_opcao_energia)                     as opcao_energia,
        trim(dsc_detalhe_mercado)                   as detalhe_mercado,

        -- valor — converte vírgula para ponto e tipifica como número
        try_to_number(
            replace(trim(vlr_mercado), ',', '.'), 18, 6
        )                                           as vlr_mercado,

        -- controle
        cast(ano_referencia as integer)             as ano_referencia

    from source
    where dat_competencia is not null
      and sig_agente_distribuidora is not null
)

select * from renamed
