with mercado as (
    select * from {{ ref('int_distribuidora_mercado_mensal') }}
),

-- Filtra apenas registros de Receita em R$
receita as (
    select
        mes_competencia,
        sigla_distribuidora,
        nome_distribuidora,
        classe_consumo,
        subgrupo_tarifario,
        ano_referencia,
        total_mercado                               as receita_reais,
        qtd_registros
    from mercado
    where detalhe_mercado ilike '%Receita%'
      and detalhe_mercado ilike '%R$%'
),

-- Acumula receita total por distribuidora e mês
final as (
    select
        mes_competencia,
        sigla_distribuidora,
        nome_distribuidora,
        ano_referencia,
        sum(receita_reais)                          as receita_total_reais,
        sum(qtd_registros)                          as qtd_registros,

        -- ranking de receita no mês (1 = maior receita)
        rank() over (
            partition by mes_competencia
            order by sum(receita_reais) desc
        )                                           as ranking_receita_mes

    from receita
    group by 1, 2, 3, 4
)

select * from final
