with samp as (
    select * from {{ ref('stg_aneel__samp') }}
    where vlr_mercado is not null
),

-- Agrega por distribuidora, mês e tipo de métrica
agregado as (
    select
        date_trunc('month', dat_competencia)        as mes_competencia,
        sigla_distribuidora,
        nome_distribuidora,
        classe_consumo,
        subgrupo_tarifario,
        tipo_mercado,
        detalhe_mercado,
        ano_referencia,

        sum(vlr_mercado)                            as total_mercado,
        count(*)                                    as qtd_registros

    from samp
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select * from agregado
