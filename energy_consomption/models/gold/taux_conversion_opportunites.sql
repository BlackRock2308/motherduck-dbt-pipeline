{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'metrics', 'conversion']
    )
}}

WITH opportunites AS (
    SELECT * FROM {{ ref('opportunites_enrichies') }}
),

-- Calcul des taux de conversion par différentes dimensions
conversions AS (
    SELECT
        -- Dimensions temporelles
        mois_creation,
        annee_creation,
        
        -- Dimensions d'origine
        origine,
        
        -- Dimensions de segment client
        segment_age,
        segment_revenus,
        segment_emploi,
        
        -- Dimensions produit
        type_projet,
        usage_bien,
        
        -- Nombre total d'opportunités
        COUNT(*) AS total_opportunites,
        
        -- Conversion commerciale
        SUM(is_converted) AS nombre_converties,
        SUM(is_converted) / CAST(COUNT(*) AS FLOAT) * 100 AS taux_conversion,
        
        -- Propositions générées
        SUM(has_propositions) AS nombre_avec_propositions,
        SUM(has_propositions) / CAST(COUNT(*) AS FLOAT) * 100 AS taux_generation_propositions,
        
        -- Montants moyens
        AVG(CASE WHEN is_converted = 1 THEN montant_total_pret ELSE NULL END) AS montant_moyen_converties,
        AVG(montant_total_pret) AS montant_moyen_global,
        
        -- Revenus et profitabilité
        SUM(esperance_gain_plateforme) AS esperance_gain_total,
        AVG(CASE WHEN is_converted = 1 THEN esperance_gain_plateforme ELSE NULL END) AS esperance_gain_moyen_opportunites_converties,
        
        -- Données de traçabilité
        MAX(_loaded_at) AS derniere_mise_a_jour
    FROM opportunites
    GROUP BY 
        mois_creation,
        annee_creation,
        origine,
        segment_age,
        segment_revenus,
        segment_emploi,
        type_projet,
        usage_bien
)

SELECT * FROM conversions