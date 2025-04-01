{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'metrics', 'acquisition']
    )
}}

WITH opportunites AS (
    SELECT * FROM {{ ref('opportunites_enrichies') }}
),

propositions AS (
    SELECT * FROM {{ ref('propositions_enrichies') }}
),

-- Agrégation des opportunités par source d'acquisition
performance_source AS (
    SELECT
        o.origine,
        DATE_TRUNC('month', o.date_creation) AS mois_acquisition,
        
        -- Volume d'acquisitions
        COUNT(DISTINCT o.opportunity_id) AS nombre_opportunites,
        
        -- Conversions
        SUM(o.is_converted) AS nombre_converties,
        SUM(o.is_converted) / CAST(COUNT(DISTINCT o.opportunity_id) AS FLOAT) * 100 AS taux_conversion,
        
        -- Taux de génération de propositions
        COUNT(DISTINCT CASE WHEN p.proposition_id IS NOT NULL THEN o.opportunity_id ELSE NULL END) AS opportunites_avec_propositions,
        COUNT(DISTINCT CASE WHEN p.proposition_id IS NOT NULL THEN o.opportunity_id ELSE NULL END) / 
            CAST(COUNT(DISTINCT o.opportunity_id) AS FLOAT) * 100 AS taux_generation_propositions,
        
        -- Nombre moyen de propositions par opportunité
        COUNT(DISTINCT p.proposition_id) / CAST(COUNT(DISTINCT o.opportunity_id) AS FLOAT) AS ratio_propositions_par_opportunite,
        
        -- Valeur des opportunités
        AVG(o.montant_total_pret) AS montant_moyen_pret,
        SUM(o.esperance_gain_plateforme) AS esperance_gain_total,
        SUM(o.esperance_gain_plateforme) / COUNT(DISTINCT o.opportunity_id) AS esperance_gain_moyen_par_opportunite,
        
        -- ROI théorique (basé sur l'espérance de gain)
        SUM(CASE WHEN o.is_converted = 1 THEN o.esperance_gain_plateforme ELSE 0 END) AS gain_realise,
        
        -- Données de traçabilité
        MAX(o._loaded_at) AS derniere_mise_a_jour
    FROM opportunites o
    LEFT JOIN propositions p ON o.opportunity_id = p.opportunity_id
    GROUP BY
        o.origine,
        DATE_TRUNC('month', o.date_creation)
)

SELECT * FROM performance_source